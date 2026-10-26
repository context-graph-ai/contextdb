//! Updates, duplicate producers, and principal gates preserve one mixed-door edge.
use super::*;
use contextdb_core::Principal;
use contextdb_engine::ReadSessionOptions;

fn fixture(path: Option<&Path>) -> Database {
    let db = path.map_or_else(Database::open_memory, |path| Database::open(path).unwrap());
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(PLAIN_EDGES, &HashMap::new()).unwrap();
    for (id, name) in [(SOURCE, "root"), (TARGET, "child")] {
        db.execute(
            "INSERT INTO entities (id, name) VALUES ($id, $name)",
            &HashMap::from([
                ("id".into(), Value::Uuid(id)),
                ("name".into(), Value::Text(name.into())),
            ]),
        )
        .unwrap();
    }
    db
}

// Capture all read doors before checking cardinality, so one duplicate does
// not prevent the remaining doors from exercising their own execution paths.
fn record_database(db: &Database, label: &str, answers: &mut Vec<(String, usize)>) {
    answers.push((
        format!("{label}: edge_count"),
        db.edge_count(SOURCE, "DEPENDS_ON", db.snapshot()).unwrap(),
    ));
    let rows = db.execute(JOINED, &root()).unwrap().rows().to_vec();
    assert!(
        rows.iter().all(|row| row == &child(1)[0]),
        "{label}: unexpected target {rows:?}"
    );
    answers.push((format!("{label}: statement"), rows.len()));
    let walked = db
        .query_bfs(SOURCE, None, Direction::Outgoing, 1, db.snapshot())
        .unwrap();
    answers.push((
        format!("{label}: walk"),
        walked.nodes.iter().filter(|node| node.id == TARGET).count(),
    ));
    let row = db
        .point_lookup("edges", "id", &Value::Uuid(EDGE_ROW), db.snapshot())
        .unwrap()
        .unwrap();
    assert_eq!(
        row.values.get("target_id"),
        Some(&Value::Uuid(TARGET)),
        "{label}: point lookup"
    );
}

fn record_session(session: &ReadSession, label: &str, answers: &mut Vec<(String, usize)>) {
    let rows = session.execute(JOINED, &root()).unwrap().rows().to_vec();
    assert!(
        rows.iter().all(|row| row == &child(1)[0]),
        "{label}: unexpected target {rows:?}"
    );
    answers.push((format!("{label}: file statement"), rows.len()));
    let cursor = session.open_cursor(JOINED, &root()).unwrap();
    assert!(
        !cursor.first_page().has_more(),
        "{label}: the bindings fit in the first page"
    );
    assert!(
        cursor
            .first_page()
            .rows()
            .iter()
            .all(|row| row == &child(1)[0])
    );
    answers.push((
        format!("{label}: file cursor"),
        cursor.first_page().rows().len(),
    ));
}

fn assert_one_binding(answers: Vec<(String, usize)>, checks: Vec<(String, bool)>) {
    assert!(!answers.is_empty());
    assert!(
        answers.iter().all(|(_, count)| *count == 1) && checks.iter().all(|(_, passed)| *passed),
        "every door owes one binding and its promised values; counts {answers:?}; value checks {checks:?}"
    );
}

#[test]
fn a_graph_property_update_keeps_the_mixed_edge_visible_once_through_every_door() {
    let mut answers = Vec::new();
    let mut checks = Vec::new();
    for on_disk in [false, true] {
        for graph_first in [true, false] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("property-update.db");
            let db = fixture(on_disk.then_some(path.as_path()));
            write_through_both_doors(&db, graph_first);
            let label = format!("disk={on_disk}, graph_first={graph_first}");
            record_database(&db, &format!("{label}: before update"), &mut answers);
            let updated = HashMap::from([("weight".to_owned(), Value::Float64(0.75))]);
            let tx = db.begin().unwrap();
            assert!(
                !db.insert_edge(tx, SOURCE, TARGET, "DEPENDS_ON".into(), updated.clone())
                    .unwrap(),
                "an update does not insert a new edge"
            );
            db.commit(tx).unwrap();
            let properties = db
                .get_edge_properties(SOURCE, TARGET, "DEPENDS_ON", db.snapshot())
                .unwrap();
            checks.push((
                format!("{label}: updated properties {properties:?}"),
                properties == Some(updated.clone()),
            ));
            record_database(&db, &format!("{label}: after update"), &mut answers);
            db.close().unwrap();
            if on_disk {
                let session = ReadSession::open(&path).unwrap();
                assert_eq!(
                    session.route(),
                    contextdb_core::read_contract::ReadRoute::File
                );
                record_session(&session, &label, &mut answers);
                drop(session);
                let reopened = Database::open(&path).unwrap();
                let properties = reopened
                    .get_edge_properties(SOURCE, TARGET, "DEPENDS_ON", reopened.snapshot())
                    .unwrap();
                checks.push((
                    format!("{label}: reopened updated properties {properties:?}"),
                    properties == Some(updated),
                ));
                record_database(&reopened, &format!("{label}: reopened"), &mut answers);
                reopened.close().unwrap();
            }
        }
    }
    assert_one_binding(answers, checks);
}

#[test]
fn a_duplicate_sql_producer_after_mixed_writes_inserts_no_relational_row_or_binding() {
    let mut answers = Vec::new();
    let mut checks = Vec::new();
    let mut affected = Vec::new();
    for on_disk in [false, true] {
        for graph_first in [true, false] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("duplicate-producer.db");
            let db = fixture(on_disk.then_some(path.as_path()));
            write_through_both_doors(&db, graph_first);
            let label = format!("disk={on_disk}, graph_first={graph_first}");
            let result = db.execute("INSERT INTO edges (id, source_id, target_id, edge_type) VALUES ($id, $source_id, $target_id, $edge_type)",
                &edge_row(Uuid::from_u128(0xd0e1), None)).unwrap();
            affected.push((label.clone(), result.rows_affected()));
            let rows = db
                .execute("SELECT id FROM edges", &HashMap::new())
                .unwrap()
                .rows()
                .to_vec();
            checks.push((
                format!("{label}: relational ids {rows:?}"),
                rows == vec![vec![Value::Uuid(EDGE_ROW)]],
            ));
            answers.push((format!("{label}: relational rows"), rows.len()));
            record_database(&db, &label, &mut answers);
            db.close().unwrap();
            if on_disk {
                let session = ReadSession::open(&path).unwrap();
                record_session(&session, &label, &mut answers);
                let rows = session
                    .execute("SELECT id FROM edges", &HashMap::new())
                    .unwrap()
                    .rows()
                    .to_vec();
                checks.push((
                    format!("{label}: file relational ids {rows:?}"),
                    rows == vec![vec![Value::Uuid(EDGE_ROW)]],
                ));
            }
        }
    }
    checks.push((
        format!("duplicate producer rows_affected {affected:?}"),
        affected.iter().all(|(_, count)| *count == 0),
    ));
    assert_one_binding(answers, checks);
}

#[test]
fn memory_file_and_principal_gated_views_agree_on_the_single_mixed_binding() {
    let mut answers = Vec::new();
    let checks = Vec::new();
    for on_disk in [false, true] {
        for graph_first in [true, false] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("gated-binding.db");
            let db = if on_disk {
                Database::open(&path).unwrap()
            } else {
                Database::open_memory()
            };
            db.execute("CREATE TABLE acl_grants (id UUID PRIMARY KEY, principal_kind TEXT, principal_id TEXT, acl_id UUID)",
                &HashMap::new()).unwrap();
            db.execute("CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT, acl_id UUID ACL REFERENCES acl_grants(acl_id))",
                &HashMap::new()).unwrap();
            db.execute(PLAIN_EDGES, &HashMap::new()).unwrap();
            let allowed_acl = Uuid::from_u128(0xda01);
            let hidden_acl = Uuid::from_u128(0xda02);
            let hidden = Uuid::from_u128(0xd003);
            db.execute("INSERT INTO acl_grants (id, principal_kind, principal_id, acl_id) VALUES ($id, 'Agent', 'reader', $acl)",
                &HashMap::from([("id".into(), Value::Uuid(Uuid::from_u128(0xda03))), ("acl".into(), Value::Uuid(allowed_acl))])).unwrap();
            for (id, name, acl) in [
                (SOURCE, "root", allowed_acl),
                (TARGET, "child", allowed_acl),
                (hidden, "hidden", hidden_acl),
            ] {
                db.execute(
                    "INSERT INTO entities (id, name, acl_id) VALUES ($id, $name, $acl)",
                    &HashMap::from([
                        ("id".into(), Value::Uuid(id)),
                        ("name".into(), Value::Text(name.into())),
                        ("acl".into(), Value::Uuid(acl)),
                    ]),
                )
                .unwrap();
            }
            write_through_both_doors(&db, graph_first);
            // A second target proves the principal gate really removes data.
            let tx = db.begin().unwrap();
            db.insert_edge(tx, SOURCE, hidden, "DEPENDS_ON".into(), weight())
                .unwrap();
            let mut hidden_row = edge_row(Uuid::from_u128(0xd0e2), None);
            hidden_row.insert("target_id".into(), Value::Uuid(hidden));
            db.insert_row(tx, "edges", hidden_row).unwrap();
            db.commit(tx).unwrap();
            let unrestricted = db.execute(JOINED, &root()).unwrap().rows().to_vec();
            assert!(
                unrestricted.contains(&vec![Value::Text("hidden".into())]),
                "the administrative control sees the hidden target"
            );
            let label = format!("disk={on_disk}, graph_first={graph_first}: principal");
            let principal = Principal::Agent("reader".into());
            let scoped = db.scoped_with_constraints(None, None, Some(principal.clone()));
            record_database(&scoped, &label, &mut answers);
            assert_eq!(
                scoped
                    .get_edge_properties(SOURCE, hidden, "DEPENDS_ON", scoped.snapshot())
                    .unwrap(),
                None,
                "hidden edge properties stay hidden"
            );

            let walk = scoped
                .query_bfs(SOURCE, None, Direction::Outgoing, 1, scoped.snapshot())
                .unwrap();
            assert!(
                !walk.nodes.iter().any(|node| node.id == hidden),
                "the principal cannot walk to the hidden target"
            );
            drop(scoped);
            db.close().unwrap();
            if on_disk {
                let session = ReadSession::open_with_options(
                    &path,
                    ReadSessionOptions {
                        principal: Some(principal.clone()),
                        ..ReadSessionOptions::default()
                    },
                )
                .unwrap();
                assert_eq!(
                    session.route(),
                    contextdb_core::read_contract::ReadRoute::File
                );
                record_session(&session, &label, &mut answers);
                drop(session);
                let reopened = Database::open_as_principal(&path, principal).unwrap();
                record_database(&reopened, &format!("{label}: reopened"), &mut answers);
                reopened.close().unwrap();
            }
        }
    }
    assert_one_binding(answers, checks);
}
