use contextdb_core::types::{ContextId, Principal, ScopeLabel};
use contextdb_core::{Direction, Error, RowId, Value, VectorIndexRef};
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

fn assert_acl_denied(err: Error, table: &str, row_id: RowId, principal: Principal, message: &str) {
    match err {
        Error::AclDenied {
            table: got_table,
            row_id: got_row_id,
            principal: got_principal,
        } => {
            assert_eq!(got_table, table, "{message}: table mismatch");
            assert_eq!(got_row_id, row_id, "{message}: row id mismatch");
            assert_eq!(got_principal, principal, "{message}: principal mismatch");
        }
        other => panic!("{message}: expected AclDenied, got {other:?}"),
    }
}

fn seed_acl_db(path: &std::path::Path) {
    let admin = Database::open(path).unwrap();
    admin.execute(
        "CREATE TABLE acl_grants (id UUID PRIMARY KEY, principal_kind TEXT, principal_id TEXT, acl_id UUID)",
        &empty(),
    ).unwrap();
    admin.execute(
        "CREATE TABLE memos (id UUID PRIMARY KEY, body TEXT, acl_id UUID ACL REFERENCES acl_grants(acl_id))",
        &empty(),
    ).unwrap();
    let acl_a = Uuid::from_u128(0xA);
    let acl_b = Uuid::from_u128(0xB);
    // Grant A to agent "a1"; grant B to agent "a2".
    for (gid, kind, pid, aid) in [
        (Uuid::from_u128(100), "Agent", "a1", acl_a),
        (Uuid::from_u128(101), "Agent", "a2", acl_b),
    ] {
        let mut p = HashMap::new();
        p.insert("id".into(), Value::Uuid(gid));
        p.insert("k".into(), Value::Text(kind.into()));
        p.insert("pid".into(), Value::Text(pid.into()));
        p.insert("aid".into(), Value::Uuid(aid));
        admin.execute(
            "INSERT INTO acl_grants (id, principal_kind, principal_id, acl_id) VALUES ($id, $k, $pid, $aid)",
            &p,
        ).unwrap();
    }
    // Memos: row 1 in acl_a, row 2 in acl_b.
    for (i, aid) in [(1u128, acl_a), (2u128, acl_b)] {
        let mut p = HashMap::new();
        p.insert("id".into(), Value::Uuid(Uuid::from_u128(i)));
        p.insert("body".into(), Value::Text(format!("body-{i}")));
        p.insert("aid".into(), Value::Uuid(aid));
        admin
            .execute(
                "INSERT INTO memos (id, body, acl_id) VALUES ($id, $body, $aid)",
                &p,
            )
            .unwrap();
    }
    admin.close().unwrap();
}

/// RED — t9_01
#[test]
fn t9_01_unprincipalled_handle_refused_on_acl_table() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("acl.redb");
    seed_acl_db(&path);
    let h = Database::open(&path).unwrap();
    // A plain `open()` is admin — it should NOT be refused. The test exercises a
    // dedicated "no principal but not admin" mode via Principal::System on a
    // handle that opted into principal-mode.
    let admin_rows = h.execute("SELECT body FROM memos", &empty()).unwrap();
    assert_eq!(
        admin_rows.rows.len(),
        2,
        "plain open is admin and bypasses ACL gating"
    );
    // We model the refusal via open_as_principal(Principal::System) where System
    // intentionally does not pass ACL checks for ACL-protected tables.
    drop(h);
    let h = Database::open_as_principal(&path, Principal::System).unwrap();
    let err = assert_error(
        h.execute("SELECT body FROM memos", &empty()),
        "System principal must be refused on ACL-protected raw scan",
    );
    assert!(
        matches!(err, Error::PrincipalRequired { ref table } if table == "memos"),
        "got: {err}"
    );
    let err = assert_error(
        h.scan("memos", h.snapshot()),
        "System principal must also be refused on direct scan of ACL-protected table",
    );
    assert!(
        matches!(err, Error::PrincipalRequired { ref table } if table == "memos"),
        "got: {err}"
    );
}

/// RED — t9_02
#[test]
fn t9_02_principal_a_sees_only_granted_rows() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("a.redb");
    seed_acl_db(&path);
    let h = Database::open_as_principal(&path, Principal::Agent("a1".into())).unwrap();
    let r = h.execute("SELECT id FROM memos", &empty()).unwrap();
    assert_eq!(r.rows.len(), 1, "agent a1 sees exactly 1 row");
    assert_eq!(r.rows[0][0], Value::Uuid(Uuid::from_u128(1)));
    let direct = h.scan("memos", h.snapshot()).unwrap();
    assert_eq!(
        direct.len(),
        1,
        "direct scan on a principalled handle must also enforce ACL"
    );
    assert!(
        matches!(direct[0].values.get("id"), Some(Value::Uuid(u)) if *u == Uuid::from_u128(1)),
        "direct scan must return only a1's granted row; rows={direct:?}"
    );
}

/// RED — t9_03
#[test]
fn t9_03_principal_a_does_not_see_principal_b_rows() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("ab.redb");
    seed_acl_db(&path);
    let h = Database::open_as_principal(&path, Principal::Agent("a1".into())).unwrap();
    let r = h.execute("SELECT id FROM memos", &empty()).unwrap();
    let a_row = Uuid::from_u128(1);
    let b_row = Uuid::from_u128(2);
    let saw_a = r
        .rows
        .iter()
        .any(|r| matches!(&r[0], Value::Uuid(u) if *u == a_row));
    assert!(
        saw_a,
        "a1's row must be visible (rules out vacuous-empty pass)"
    );
    for row in &r.rows {
        if let Value::Uuid(u) = &row[0] {
            assert_ne!(*u, b_row, "B's row leaked");
        }
    }
}

/// RED — t9_04
#[test]
fn t9_04_principal_a_cannot_write_to_b_row() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("write.redb");
    seed_acl_db(&path);
    let denied_acl = Uuid::from_u128(0xB);
    let granted_acl = Uuid::from_u128(0xA);
    let granted_row = Uuid::from_u128(1);
    let target_row = Uuid::from_u128(2);
    let admin = Database::open(&path).unwrap();
    let rows = admin.scan("memos", admin.snapshot()).unwrap();
    let granted_row_id = rows
        .iter()
        .find(|row| matches!(row.values.get("id"), Some(Value::Uuid(u)) if *u == granted_row))
        .expect("acl_a fixture row missing")
        .row_id;
    let target_row_id = rows
        .iter()
        .find(|row| matches!(row.values.get("id"), Some(Value::Uuid(u)) if *u == target_row))
        .expect("acl_b fixture row missing")
        .row_id;
    drop(admin);
    let h = Database::open_as_principal(&path, Principal::Agent("a1".into())).unwrap();

    let mut insert = HashMap::new();
    insert.insert("id".into(), Value::Uuid(Uuid::from_u128(3)));
    insert.insert("body".into(), Value::Text("plant".into()));
    insert.insert("aid".into(), Value::Uuid(denied_acl));
    let err = assert_error(
        h.execute(
            "INSERT INTO memos (id, body, acl_id) VALUES ($id, $body, $aid)",
            &insert,
        ),
        "agent a1 cannot insert a new row under acl_b",
    );
    match err {
        Error::AclDenied {
            table,
            row_id,
            principal,
        } => {
            assert_eq!(table, "memos", "AclDenied must carry the table name");
            assert_ne!(
                row_id,
                RowId(0),
                "AclDenied for denied INSERT must carry a non-bogus candidate row id"
            );
            assert!(
                matches!(principal, Principal::Agent(ref a) if a == "a1"),
                "AclDenied must carry the writer's principal; got {principal:?}"
            );
        }
        other => panic!("expected AclDenied for denied INSERT, got: {other:?}"),
    }

    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(target_row));
    p.insert("body".into(), Value::Text("hijack".into()));
    let err = assert_error(
        h.execute("UPDATE memos SET body = $body WHERE id = $id", &p),
        "agent a1 cannot write to acl_b row",
    );
    match err {
        Error::AclDenied {
            table,
            row_id,
            principal,
        } => {
            assert_eq!(table, "memos", "AclDenied must carry the table name");
            assert_eq!(
                row_id, target_row_id,
                "AclDenied for denied UPDATE must carry the existing row id"
            );
            assert!(
                matches!(principal, Principal::Agent(ref a) if a == "a1"),
                "AclDenied must carry the writer's principal; got {principal:?}"
            );
        }
        other => panic!("expected AclDenied, got: {other:?}"),
    }
    let mut flip = HashMap::new();
    flip.insert("id".into(), Value::Uuid(granted_row));
    flip.insert("aid".into(), Value::Uuid(denied_acl));
    let err = assert_error(
        h.execute("UPDATE memos SET acl_id = $aid WHERE id = $id", &flip),
        "agent a1 cannot flip an acl_a row into acl_b",
    );
    assert_acl_denied(
        err,
        "memos",
        granted_row_id,
        Principal::Agent("a1".into()),
        "ACL write gate must evaluate UPDATE post-image, not only the pre-image",
    );
    let err = assert_error(
        h.execute("DELETE FROM memos WHERE id = $id", &p),
        "agent a1 cannot delete acl_b row",
    );
    match err {
        Error::AclDenied {
            table,
            row_id,
            principal,
        } => {
            assert_eq!(table, "memos", "AclDenied must carry the table name");
            assert_eq!(
                row_id, target_row_id,
                "AclDenied for denied DELETE must carry the existing row id"
            );
            assert!(
                matches!(principal, Principal::Agent(ref a) if a == "a1"),
                "AclDenied must carry the writer's principal; got {principal:?}"
            );
        }
        other => panic!("expected AclDenied for denied DELETE, got: {other:?}"),
    }

    drop(h);
    let admin = Database::open(&path).unwrap();
    let r = admin
        .execute("SELECT id, body, acl_id FROM memos", &empty())
        .unwrap();
    assert!(
        !r.rows
            .iter()
            .any(|row| row[0] == Value::Uuid(Uuid::from_u128(3))),
        "denied INSERT under acl_b must not persist row 3; rows={:?}",
        r.rows
    );
    assert!(
        r.rows.iter().any(|row| {
            row[0] == Value::Uuid(target_row) && row[1] == Value::Text("body-2".into())
        }),
        "denied UPDATE must leave acl_b row unchanged; rows={:?}",
        r.rows
    );
    assert!(
        r.rows.iter().any(|row| {
            row[0] == Value::Uuid(granted_row)
                && row[1] == Value::Text("body-1".into())
                && row[2] == Value::Uuid(granted_acl)
        }),
        "denied ACL flip must leave granted row in acl_a with its original body; rows={:?}",
        r.rows
    );
}

/// RED — t9_05
#[test]
fn t9_05_graph_traversal_excludes_acl_denied_rows() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("graph.redb");
    seed_acl_db(&path);
    let denied_row_id = {
        let admin = Database::open(&path).unwrap();
        admin.execute(
            "CREATE TABLE memos_edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
            &empty(),
        ).unwrap();
        let mut memo = HashMap::new();
        memo.insert("id".into(), Value::Uuid(Uuid::from_u128(3)));
        memo.insert("body".into(), Value::Text("body-3".into()));
        memo.insert("aid".into(), Value::Uuid(Uuid::from_u128(0xA)));
        admin
            .execute(
                "INSERT INTO memos (id, body, acl_id) VALUES ($id, $body, $aid)",
                &memo,
            )
            .unwrap();
        memo.insert("id".into(), Value::Uuid(Uuid::from_u128(4)));
        memo.insert("body".into(), Value::Text("body-4".into()));
        admin
            .execute(
                "INSERT INTO memos (id, body, acl_id) VALUES ($id, $body, $aid)",
                &memo,
            )
            .unwrap();
        for (eid, src, tgt) in [
            (Uuid::from_u128(50), Uuid::from_u128(1), Uuid::from_u128(1)),
            (Uuid::from_u128(51), Uuid::from_u128(1), Uuid::from_u128(2)),
            (Uuid::from_u128(52), Uuid::from_u128(2), Uuid::from_u128(3)),
            (Uuid::from_u128(53), Uuid::from_u128(1), Uuid::from_u128(4)),
        ] {
            let mut p = HashMap::new();
            p.insert("eid".into(), Value::Uuid(eid));
            p.insert("src".into(), Value::Uuid(src));
            p.insert("tgt".into(), Value::Uuid(tgt));
            admin.execute(
                "INSERT INTO memos_edges (id, source_id, target_id, edge_type) VALUES ($eid, $src, $tgt, 'LINKS')",
                &p,
            ).unwrap();
        }
        let denied_row_id = admin
            .scan("memos", admin.snapshot())
            .unwrap()
            .into_iter()
            .find(|row| {
                matches!(row.values.get("id"), Some(Value::Uuid(u)) if *u == Uuid::from_u128(2))
            })
            .expect("ACL-denied graph fixture row missing")
            .row_id;
        admin.close().unwrap();
        denied_row_id
    };
    let h = Database::open_as_principal(&path, Principal::Agent("a1".into())).unwrap();
    let r = h.execute(
        "SELECT b_id FROM GRAPH_TABLE(memos_edges MATCH (a)-[:LINKS]->{1,2}(b) COLUMNS (b.id AS b_id))",
        &empty(),
    ).unwrap();
    let a_row = Uuid::from_u128(1);
    let b_row = Uuid::from_u128(2);
    let a_row_behind_b = Uuid::from_u128(3);
    let a_allowed_neighbor = Uuid::from_u128(4);
    let saw_a = r
        .rows
        .iter()
        .any(|r| matches!(&r[0], Value::Uuid(u) if *u == a_row));
    assert!(saw_a, "a1's node must be visible in graph traversal");
    let saw_allowed_neighbor = r
        .rows
        .iter()
        .any(|r| matches!(&r[0], Value::Uuid(u) if *u == a_allowed_neighbor));
    assert!(
        saw_allowed_neighbor,
        "ACL graph traversal must follow at least one allowed edge; rows={:?}",
        r.rows
    );
    for row in &r.rows {
        if let Value::Uuid(u) = &row[0] {
            assert_ne!(*u, b_row, "ACL-denied node leaked");
            assert_ne!(
                *u, a_row_behind_b,
                "ACL-granted node reachable only through denied bridge leaked"
            );
        }
    }
    let direct = h
        .query_bfs(a_row, None, Direction::Outgoing, 2, h.snapshot())
        .unwrap();
    let direct_ids: Vec<Uuid> = direct.nodes.iter().map(|node| node.id).collect();
    assert!(
        direct_ids.contains(&a_row),
        "direct graph helper must still return a1's granted node; nodes={direct_ids:?}"
    );
    assert!(
        direct_ids.contains(&a_allowed_neighbor),
        "direct graph helper must traverse allowed ACL-granted neighbor; nodes={direct_ids:?}"
    );
    assert!(
        !direct_ids.contains(&b_row) && !direct_ids.contains(&a_row_behind_b),
        "direct graph helper leaked ACL-denied nodes; nodes={direct_ids:?}"
    );

    let denied_insert_source = a_allowed_neighbor;
    let denied_insert_target = b_row;
    let tx = h.begin();
    let err = assert_error(
        h.insert_edge(
            tx,
            denied_insert_source,
            denied_insert_target,
            "DENIED_LINK".into(),
            HashMap::new(),
        ),
        "direct insert_edge by a1 to ACL-denied node must be rejected",
    );
    assert_acl_denied(
        err,
        "memos",
        denied_row_id,
        Principal::Agent("a1".into()),
        "direct insert_edge by a1 to ACL-denied node",
    );
    h.commit(tx).unwrap();

    let tx = h.begin();
    let err = assert_error(
        h.delete_edge(tx, a_row, b_row, "LINKS"),
        "direct delete_edge by a1 touching ACL-denied node must be rejected",
    );
    assert_acl_denied(
        err,
        "memos",
        denied_row_id,
        Principal::Agent("a1".into()),
        "direct delete_edge by a1 touching ACL-denied node",
    );
    h.commit(tx).unwrap();
    drop(h);

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
        "failed direct insert_edge must not leave a denied ACL edge"
    );
    assert!(
        admin
            .get_edge_properties(a_row, b_row, "LINKS", admin.snapshot())
            .unwrap()
            .is_some(),
        "failed direct delete_edge must not remove the existing ACL-denied edge after commit"
    );
}

/// RED — t9_06
#[test]
fn t9_06_vector_query_excludes_acl_denied_rows() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("vec.redb");
    let (a_far_row_id, a_nearest_row_id, denied_row_ids) = {
        let admin = Database::open(&path).unwrap();
        admin.execute(
            "CREATE TABLE acl_grants (id UUID PRIMARY KEY, principal_kind TEXT, principal_id TEXT, acl_id UUID)",
            &empty(),
        ).unwrap();
        admin
            .execute(
                "CREATE TABLE memos (id UUID PRIMARY KEY, embedding VECTOR(3), \
             acl_id UUID ACL REFERENCES acl_grants(acl_id))",
                &empty(),
            )
            .unwrap();
        let acl_a = Uuid::from_u128(0xA);
        let acl_b = Uuid::from_u128(0xB);
        let mut g = HashMap::new();
        g.insert("id".into(), Value::Uuid(Uuid::from_u128(100)));
        g.insert("k".into(), Value::Text("Agent".into()));
        g.insert("pid".into(), Value::Text("a1".into()));
        g.insert("aid".into(), Value::Uuid(acl_a));
        admin.execute(
            "INSERT INTO acl_grants (id, principal_kind, principal_id, acl_id) VALUES ($id, $k, $pid, $aid)",
            &g,
        ).unwrap();
        let rows = std::iter::once((1u128, acl_a, vec![1.0, 0.0, 0.0]))
            .chain((2u128..12).map(|i| (i, acl_b, vec![0.0, 1.0, 0.0])))
            .chain(std::iter::once((12u128, acl_a, vec![0.0, 1.0, 0.0])))
            .chain(std::iter::once((13u128, acl_a, vec![0.5, 0.5, 0.0])));
        for (i, aid, vector) in rows {
            let mut p = HashMap::new();
            p.insert("id".into(), Value::Uuid(Uuid::from_u128(i)));
            p.insert("v".into(), Value::Vector(vector));
            p.insert("aid".into(), Value::Uuid(aid));
            admin
                .execute(
                    "INSERT INTO memos (id, embedding, acl_id) VALUES ($id, $v, $aid)",
                    &p,
                )
                .unwrap();
        }
        let rows = admin.scan("memos", admin.snapshot()).unwrap();
        let a_far_row_id = rows
            .iter()
            .find(|row| matches!(row.values.get("id"), Some(Value::Uuid(u)) if *u == Uuid::from_u128(1)))
            .expect("ACL far vector fixture row missing")
            .row_id;
        let a_nearest_row_id = rows
            .iter()
            .find(|row| matches!(row.values.get("id"), Some(Value::Uuid(u)) if *u == Uuid::from_u128(12)))
            .expect("ACL nearest vector fixture row missing")
            .row_id;
        let denied_row_ids: BTreeSet<_> = rows
            .iter()
            .filter(|row| matches!(row.values.get("acl_id"), Some(Value::Uuid(u)) if *u == acl_b))
            .map(|row| row.row_id)
            .collect();
        admin.close().unwrap();
        (a_far_row_id, a_nearest_row_id, denied_row_ids)
    };
    let h = Database::open_as_principal(&path, Principal::Agent("a1".into())).unwrap();
    let mut p = HashMap::new();
    p.insert("q".into(), Value::Vector(vec![0.0, 1.0, 0.0]));
    let r = h
        .execute("SELECT id FROM memos ORDER BY embedding <=> $q LIMIT 1", &p)
        .unwrap();
    let a_row = Uuid::from_u128(12);
    assert_eq!(
        r.rows.len(),
        1,
        "ACL vector gating must happen before LIMIT"
    );
    let saw_a = r
        .rows
        .iter()
        .any(|r| matches!(&r[0], Value::Uuid(u) if *u == a_row));
    assert!(
        saw_a,
        "nearest ACL-granted row must be present in vector ranking"
    );
    for row in &r.rows {
        if let Value::Uuid(u) = &row[0] {
            assert!(
                !(2..12).contains(&u.as_u128()),
                "ACL-denied row leaked into vector ranking"
            );
        }
    }
    let direct = h
        .query_vector(
            VectorIndexRef::new("memos", "embedding"),
            &[0.0, 1.0, 0.0],
            10,
            None,
            h.snapshot(),
        )
        .unwrap();
    assert!(
        matches!(direct.first(), Some((row_id, _)) if *row_id == a_nearest_row_id),
        "direct vector API must rank the nearest ACL-granted row first; far={a_far_row_id:?} result={direct:?}"
    );
    assert!(
        direct
            .iter()
            .all(|(row_id, _)| !denied_row_ids.contains(row_id)),
        "direct vector API leaked ACL-denied rows; result={direct:?}"
    );

    let mut denied_rows = denied_row_ids.iter();
    let denied_insert_row_id = *denied_rows
        .next()
        .expect("ACL-denied vector fixture row missing for insert");
    let denied_delete_row_id = *denied_rows
        .next()
        .expect("ACL-denied vector fixture row missing for delete");
    let tx = h.begin();
    let err = assert_error(
        h.insert_vector(
            tx,
            VectorIndexRef::new("memos", "embedding"),
            denied_insert_row_id,
            vec![1.0, 0.0, 0.0],
        ),
        "direct insert_vector by a1 into ACL-denied row must be rejected",
    );
    assert_acl_denied(
        err,
        "memos",
        denied_insert_row_id,
        Principal::Agent("a1".into()),
        "direct insert_vector by a1 into ACL-denied row",
    );
    h.commit(tx).unwrap();

    let tx = h.begin();
    let err = assert_error(
        h.delete_vector(
            tx,
            VectorIndexRef::new("memos", "embedding"),
            denied_delete_row_id,
        ),
        "direct delete_vector by a1 on ACL-denied row must be rejected",
    );
    assert_acl_denied(
        err,
        "memos",
        denied_delete_row_id,
        Principal::Agent("a1".into()),
        "direct delete_vector by a1 on ACL-denied row",
    );
    h.commit(tx).unwrap();
    drop(h);

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
        "failed direct insert_vector must not re-vector an ACL-denied row; result={inserted_query:?}"
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
        "failed direct delete_vector must not remove the ACL-denied row; result={deleted_query:?}"
    );
}

/// REGRESSION GUARD — t9_07
#[test]
fn t9_07_non_acl_table_unaffected() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("plain.redb");
    {
        let admin = Database::open(&path).unwrap();
        admin
            .execute(
                "CREATE TABLE plain (id UUID PRIMARY KEY, v TEXT, acl_id UUID)",
                &empty(),
            )
            .unwrap();
        let mut p = HashMap::new();
        p.insert("id".into(), Value::Uuid(Uuid::from_u128(1)));
        p.insert("v".into(), Value::Text("ok".into()));
        p.insert("acl".into(), Value::Uuid(Uuid::from_u128(0xBAD)));
        admin
            .execute(
                "INSERT INTO plain (id, v, acl_id) VALUES ($id, $v, $acl)",
                &p,
            )
            .unwrap();
        admin.close().unwrap();
    }
    let h = Database::open_as_principal(&path, Principal::Agent("a1".into())).unwrap();
    let r = h.execute("SELECT v, acl_id FROM plain", &empty()).unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Text("ok".into()));
    assert_eq!(r.rows[0][1], Value::Uuid(Uuid::from_u128(0xBAD)));
}

/// RED — t9_08
#[test]
fn t9_08_principal_variant_drives_acl_via_schema() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("variant.redb");
    let acl_a = Uuid::from_u128(0xA);
    let acl_h = Uuid::from_u128(0xC);
    let acl_shared_agent = Uuid::from_u128(0xD);
    let acl_shared_human = Uuid::from_u128(0xE);
    {
        let admin = Database::open(&path).unwrap();
        admin.execute(
            "CREATE TABLE acl_grants (id UUID PRIMARY KEY, principal_kind TEXT, principal_id TEXT, acl_id UUID)",
            &empty(),
        ).unwrap();
        admin
            .execute(
                "CREATE TABLE memos (id UUID PRIMARY KEY, body TEXT, \
             acl_id UUID ACL REFERENCES acl_grants(acl_id))",
                &empty(),
            )
            .unwrap();
        for (gid, kind, pid, aid) in [
            (Uuid::from_u128(100), "Agent", "a1", acl_a),
            (Uuid::from_u128(101), "Human", "alice@x", acl_h),
            (Uuid::from_u128(102), "Agent", "shared", acl_shared_agent),
            (Uuid::from_u128(103), "Human", "shared", acl_shared_human),
        ] {
            let mut p = HashMap::new();
            p.insert("id".into(), Value::Uuid(gid));
            p.insert("k".into(), Value::Text(kind.into()));
            p.insert("pid".into(), Value::Text(pid.into()));
            p.insert("aid".into(), Value::Uuid(aid));
            admin.execute(
                "INSERT INTO acl_grants (id, principal_kind, principal_id, acl_id) VALUES ($id, $k, $pid, $aid)",
                &p,
            ).unwrap();
        }
        for (i, aid) in [
            (1u128, acl_a),
            (2u128, acl_h),
            (3u128, acl_shared_agent),
            (4u128, acl_shared_human),
        ] {
            let mut p = HashMap::new();
            p.insert("id".into(), Value::Uuid(Uuid::from_u128(i)));
            p.insert("b".into(), Value::Text(format!("r{i}")));
            p.insert("aid".into(), Value::Uuid(aid));
            admin
                .execute(
                    "INSERT INTO memos (id, body, acl_id) VALUES ($id, $b, $aid)",
                    &p,
                )
                .unwrap();
        }
        admin.close().unwrap();
    }
    let h_agent = Database::open_as_principal(&path, Principal::Agent("a1".into())).unwrap();
    let r1 = h_agent.execute("SELECT id FROM memos", &empty()).unwrap();
    assert_eq!(r1.rows.len(), 1);
    assert_eq!(r1.rows[0][0], Value::Uuid(Uuid::from_u128(1)));
    drop(h_agent);

    let h_human = Database::open_as_principal(&path, Principal::Human("alice@x".into())).unwrap();
    let r2 = h_human.execute("SELECT id FROM memos", &empty()).unwrap();
    assert_eq!(r2.rows.len(), 1);
    assert_eq!(r2.rows[0][0], Value::Uuid(Uuid::from_u128(2)));
    drop(h_human);

    let shared_agent =
        Database::open_as_principal(&path, Principal::Agent("shared".into())).unwrap();
    let agent_rows = shared_agent
        .execute("SELECT id FROM memos", &empty())
        .unwrap();
    assert_eq!(
        agent_rows.rows,
        vec![vec![Value::Uuid(Uuid::from_u128(3))]],
        "Agent(shared) must match only Agent/shared grants, not Human/shared"
    );
    drop(shared_agent);

    let shared_human =
        Database::open_as_principal(&path, Principal::Human("shared".into())).unwrap();
    let human_rows = shared_human
        .execute("SELECT id FROM memos", &empty())
        .unwrap();
    assert_eq!(
        human_rows.rows,
        vec![vec![Value::Uuid(Uuid::from_u128(4))]],
        "Human(shared) must match only Human/shared grants, not Agent/shared"
    );
    drop(shared_human);

    {
        let admin = Database::open(&path).unwrap();
        admin
            .execute(
                "CREATE TABLE records (id UUID PRIMARY KEY, body TEXT, \
             context_id UUID CONTEXT_ID, \
             scope_label TEXT SCOPE_LABEL_READ ('edge', 'server') WRITE ('edge', 'server'), \
             acl_id UUID ACL REFERENCES acl_grants(acl_id))",
                &empty(),
            )
            .unwrap();
        let ctx_a = Uuid::from_u128(0xCA);
        let ctx_b = Uuid::from_u128(0xCB);
        let acl_b = Uuid::from_u128(0xB);
        for (id, ctx, scope, acl, body) in [
            (10u128, ctx_a, "edge", acl_a, "visible"),
            (11u128, ctx_b, "edge", acl_a, "context-denied"),
            (12u128, ctx_a, "server", acl_a, "scope-denied"),
            (13u128, ctx_a, "edge", acl_b, "acl-denied"),
        ] {
            let mut p = HashMap::new();
            p.insert("id".into(), Value::Uuid(Uuid::from_u128(id)));
            p.insert("body".into(), Value::Text(body.into()));
            p.insert("ctx".into(), Value::Uuid(ctx));
            p.insert("scope".into(), Value::Text(scope.into()));
            p.insert("acl".into(), Value::Uuid(acl));
            admin
                .execute(
                    "INSERT INTO records (id, body, context_id, scope_label, acl_id) \
                 VALUES ($id, $body, $ctx, $scope, $acl)",
                    &p,
                )
                .unwrap();
        }
        admin.close().unwrap();
    }

    let composed = Database::open_with_constraints(
        &path,
        Some(BTreeSet::from([ContextId::new(Uuid::from_u128(0xCA))])),
        Some(BTreeSet::from([ScopeLabel::new("edge")])),
        Some(Principal::Agent("a1".into())),
    )
    .unwrap();
    let r = composed
        .execute("SELECT id, body FROM records", &empty())
        .unwrap();
    assert_eq!(
        r.rows,
        vec![vec![
            Value::Uuid(Uuid::from_u128(10)),
            Value::Text("visible".into())
        ]],
        "context, scope-label, and ACL gates must compose on one handle"
    );
    let direct = composed.scan("records", composed.snapshot()).unwrap();
    assert_eq!(
        direct.len(),
        1,
        "direct scan must also compose context, scope-label, and ACL gates"
    );
    assert!(
        matches!(direct[0].values.get("body"), Some(Value::Text(s)) if s == "visible"),
        "direct scan must return only the composed-visible row; rows={direct:?}"
    );
}

/// REGRESSION GUARD — t9_09: principal-A can write and insert acl_a rows (granted).
#[test]
fn t9_09_principal_a_writes_granted_row() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("write_ok.redb");
    seed_acl_db(&path);
    let h = Database::open_as_principal(&path, Principal::Agent("a1".into())).unwrap();
    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(Uuid::from_u128(1)));
    p.insert("body".into(), Value::Text("updated".into()));
    h.execute("UPDATE memos SET body = $body WHERE id = $id", &p)
        .expect("a1 must be able to write to a1-granted row");
    let acl_a = Uuid::from_u128(0xA);
    let mut insert = HashMap::new();
    insert.insert("id".into(), Value::Uuid(Uuid::from_u128(10)));
    insert.insert("body".into(), Value::Text("inserted".into()));
    insert.insert("acl".into(), Value::Uuid(acl_a));
    h.execute(
        "INSERT INTO memos (id, body, acl_id) VALUES ($id, $body, $acl)",
        &insert,
    )
    .expect("a1 must be able to insert a new row under a1-granted ACL");
    let r = h.execute("SELECT id, body FROM memos", &empty()).unwrap();
    assert!(
        r.rows.iter().any(|row| {
            row.len() == 2
                && row[0] == Value::Uuid(Uuid::from_u128(1))
                && row[1] == Value::Text("updated".into())
        }),
        "granted row must be updated and visible; rows={:?}",
        r.rows
    );
    assert!(
        r.rows.iter().any(|row| {
            row.len() == 2
                && row[0] == Value::Uuid(Uuid::from_u128(10))
                && row[1] == Value::Text("inserted".into())
        }),
        "new row inserted under a granted ACL must be visible; rows={:?}",
        r.rows
    );
}

/// RED — t9_10
#[test]
fn t9_10_acl_constraint_on_acl_grants_table_rejected() {
    let db = Database::open_memory();
    let err = assert_error(
        db.execute(
            "CREATE TABLE acl_grants (acl_id UUID PRIMARY KEY ACL REFERENCES acl_grants(acl_id), principal_kind TEXT, principal_id TEXT)",
            &empty(),
        ),
        "ACL on acl_grants must be rejected at CREATE TABLE",
    );
    assert!(
        matches!(err, Error::SchemaInvalid { ref reason } if reason.contains("acl_grants")),
        "got: {err}"
    );
}
