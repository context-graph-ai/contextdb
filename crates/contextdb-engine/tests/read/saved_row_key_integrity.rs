use contextdb_core::{Value, read_contract::ReadRoute};
use contextdb_engine::direct_file_reader::test_seams::{ReadWorkObservationScope, ReadWorkPhase};
use contextdb_engine::{Database, ReadSession, ReadSessionOptions};
use redb::{ReadableTable, TableDefinition};
use std::collections::HashMap;

const QUERY: &str = "SELECT id, payload FROM key_integrity ORDER BY sequence";

fn seed(path: &std::path::Path) {
    let db = Database::open(path).expect("open writer");
    db.execute(
        "CREATE TABLE key_integrity (id INTEGER PRIMARY KEY, sequence INTEGER NOT NULL, payload TEXT NOT NULL, embedding VECTOR(2) SEARCH_MODE EXACT)",
        &HashMap::new(),
    )
    .expect("create relation");
    db.execute(
        "CREATE INDEX key_integrity_sequence ON key_integrity (sequence)",
        &HashMap::new(),
    )
    .expect("create ordered index");
    for id in 1..=2 {
        db.execute(
            "INSERT INTO key_integrity VALUES ($id, $sequence, $payload, $embedding)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("sequence".into(), Value::Int64(id)),
                ("payload".into(), Value::Text(format!("body-{id}"))),
                ("embedding".into(), Value::Vector(vec![id as f32, 0.0])),
            ]),
        )
        .expect("commit row");
    }
    db.close().expect("close seeded writer");
}

fn damage_row(path: &std::path::Path, swap_body: bool) {
    let raw = redb::Database::open(path).expect("open physical store");
    let write = raw.begin_write().expect("begin physical damage");
    {
        let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new("rel_key_integrity");
        let mut relation = write.open_table(definition).expect("open physical rows");
        let rows = relation
            .iter()
            .expect("iterate rows")
            .map(|entry| {
                let (key, body) = entry.expect("row entry");
                (key.value().to_vec(), body.value().to_vec())
            })
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), 2, "fixture has two saved versions");
        assert_eq!(rows[0].0.len(), 24, "first saved key has MVCC identity");
        assert_eq!(rows[1].0.len(), 24, "second saved key has MVCC identity");
        let first_tx = u64::from_be_bytes(rows[0].0[8..16].try_into().expect("transaction id"));
        let second_tx = u64::from_be_bytes(rows[1].0[8..16].try_into().expect("transaction id"));
        assert!(
            first_tx < second_tx,
            "the damaged key belongs to the first committed and selected row"
        );
        if swap_body {
            relation
                .insert(rows[0].0.as_slice(), rows[1].1.as_slice())
                .expect("replace the first body's bytes under its saved key");
        } else {
            relation
                .remove(rows[0].0.as_slice())
                .expect("remove exactly one saved version");
        }
    }
    write.commit().expect("commit physical damage");
}

fn assert_refused_on_route(swap_body: bool, route: ReadRoute) {
    let root = tempfile::tempdir().expect("task scoped store");
    let path = root.path().join("saved-key.db");
    seed(&path);
    let open_work = ReadWorkObservationScope::enter(ReadWorkPhase::Open);
    let clean = ReadSession::open(&path).expect("open undamaged file reader");
    assert_eq!(
        open_work.counters().decoded_rows,
        0,
        "a ready vector directory keeps relational rows lazy at file open"
    );
    drop(open_work);
    let clean_answer = clean
        .execute(QUERY, &HashMap::new())
        .expect("both saved rows are selected before damage");
    assert_eq!(
        clean_answer.rows(),
        vec![
            vec![Value::Int64(1), Value::Text("body-1".into())],
            vec![Value::Int64(2), Value::Text("body-2".into())],
        ],
        "the ordered query includes the exact row whose saved key is damaged"
    );
    drop(clean);
    let owner = if route == ReadRoute::Owner {
        let owner = Database::open(&path).expect("open owner before saved-row damage");
        owner
            .__damage_first_saved_row_for_test("key_integrity", swap_body)
            .expect("damage saved row after the owner retains its version directory");
        Some(owner)
    } else {
        damage_row(&path, swap_body);
        None
    };
    let reader = if route == ReadRoute::Owner {
        ReadSession::open_owner_only(&path, ReadSessionOptions::default())
            .expect("open owner route")
    } else {
        ReadSession::open(&path).expect("open file route")
    };
    assert_eq!(reader.route(), route);
    let outcome = reader.execute(QUERY, &HashMap::new());
    let error = outcome.expect_err("a selected saved version must be present and own its body");
    assert!(
        error.to_string().contains("corrupt"),
        "saved key damage must have typed corruption: {error}"
    );
    drop(reader);
    if let Some(owner) = owner {
        owner.close().expect("close owner");
    }
}

#[test]
fn a_missing_saved_version_is_typed_damage_on_both_bounded_routes() {
    for route in [ReadRoute::File, ReadRoute::Owner] {
        assert_refused_on_route(false, route);
    }
}

#[test]
fn a_saved_key_with_another_rows_body_is_typed_damage_on_both_bounded_routes() {
    for route in [ReadRoute::File, ReadRoute::Owner] {
        assert_refused_on_route(true, route);
    }
}

#[test]
fn an_identity_only_candidate_with_no_saved_version_resolves_its_visible_row() {
    let root = tempfile::tempdir().expect("task scoped store");
    let path = root.path().join("identity-only.db");
    let owner = Database::open(&path).expect("open writer");
    owner
        .execute(
            "CREATE TABLE identity_only (id INTEGER PRIMARY KEY, label TEXT UNIQUE NOT NULL, embedding VECTOR(2) SEARCH_MODE EXACT)",
            &HashMap::new(),
        )
        .expect("create vector candidate table");
    owner
        .execute(
            "INSERT INTO identity_only VALUES (7, 'wanted', $embedding)",
            &HashMap::from([("embedding".into(), Value::Vector(vec![1.0, 0.0]))]),
        )
        .expect("commit row");
    let reader = ReadSession::open_owner_only(&path, ReadSessionOptions::default())
        .expect("open owner route");
    assert_eq!(reader.route(), ReadRoute::Owner);
    let answer = reader
        .execute(
            "SELECT id FROM identity_only WHERE label = 'wanted' ORDER BY embedding <=> $q LIMIT 1",
            &HashMap::from([("q".into(), Value::Vector(vec![1.0, 0.0]))]),
        )
        .expect("synthetic identity cursor resolves the visible row");
    assert_eq!(answer.rows(), vec![vec![Value::Int64(7)]]);
    drop(reader);
    owner.close().expect("close owner");
}

#[test]
fn a_filtered_identity_candidate_binds_the_saved_body_to_its_physical_key() {
    let root = tempfile::tempdir().expect("task scoped store");
    let path = root.path().join("filtered-identity.db");
    let owner = Database::open(&path).expect("open writer");
    owner.execute(
        "CREATE TABLE candidates (id INTEGER PRIMARY KEY, label TEXT NOT NULL, embedding VECTOR(2) SEARCH_MODE EXACT)",
        &HashMap::new(),
    ).expect("create candidates");
    owner
        .execute(
            "CREATE INDEX candidates_label ON candidates (label)",
            &HashMap::new(),
        )
        .expect("create filter index");
    for id in 1..=2 {
        owner
            .execute(
                "INSERT INTO candidates VALUES ($id, 'wanted', $embedding)",
                &HashMap::from([
                    ("id".into(), Value::Int64(id)),
                    ("embedding".into(), Value::Vector(vec![id as f32, 0.0])),
                ]),
            )
            .expect("commit candidate");
    }
    let query =
        "SELECT id FROM candidates WHERE label = 'wanted' ORDER BY embedding <=> $q LIMIT 2";
    let params = HashMap::from([("q".into(), Value::Vector(vec![1.0, 0.0]))]);
    let reader = ReadSession::open_owner_only(&path, ReadSessionOptions::default())
        .expect("open owner route");
    let intact = reader
        .execute(query, &params)
        .expect("intact filtered query");
    assert_eq!(
        intact.rows().len(),
        2,
        "both matching identities are selected"
    );
    assert_eq!(intact.trace().index_used.as_deref(), Some("candidates_label"));
    owner
        .__damage_first_saved_row_for_test("candidates", true)
        .expect("swap the first saved body after owner open");
    let error = reader
        .execute(query, &params)
        .expect_err("selected physical identity must own its decoded body");
    assert!(
        error.to_string().contains("corrupt"),
        "typed damage: {error}"
    );
    drop(reader);
    owner.close().expect("close damaged owner");
}

#[test]
fn an_owner_physical_scan_refuses_a_missing_saved_version() {
    let root = tempfile::tempdir().expect("task scoped store");
    let path = root.path().join("physical-scan.db");
    seed(&path);
    let owner = Database::open(&path).expect("load owner version directory");
    let reader = ReadSession::open_owner_only(&path, ReadSessionOptions::default())
        .expect("open owner route");
    let query = "SELECT id, payload FROM key_integrity";
    let intact = reader.execute(query, &HashMap::new()).expect("intact scan");
    assert_eq!(intact.rows().len(), 2);
    assert_eq!(intact.trace().physical_plan, "Scan");
    owner
        .__damage_first_saved_row_for_test("key_integrity", false)
        .expect("remove saved body after owner directory loaded");
    let error = reader
        .execute(query, &HashMap::new())
        .expect_err("a physical directory entry requires its saved version");
    assert!(
        error.to_string().contains("corrupt"),
        "typed damage: {error}"
    );
    drop(reader);
    owner.close().expect("close damaged owner");
}

#[test]
fn unchanged_foreign_key_posting_tracks_an_updated_row_and_retires_on_delete() {
    let root = tempfile::tempdir().expect("task scoped store");
    let path = root.path().join("updated-fk.db");
    let writer = Database::open(&path).expect("open writer");
    for statement in [
        "CREATE TABLE parents (id INTEGER PRIMARY KEY)",
        "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id), payload TEXT)",
        "INSERT INTO parents VALUES (1)",
        "INSERT INTO children VALUES (7, 1, 'first')",
        "UPDATE children SET payload = 'second' WHERE id = 7",
        "UPDATE children SET payload = 'current' WHERE id = 7",
    ] {
        writer.execute(statement, &HashMap::new()).expect(statement);
    }
    writer.close().expect("close writer for file read");

    let reader = ReadSession::open(&path).expect("open file reader");
    assert_eq!(reader.route(), ReadRoute::File);
    let answer = reader
        .execute(
            "SELECT id, payload FROM children WHERE parent_id = 1",
            &HashMap::new(),
        )
        .expect("unchanged FK posting resolves the current visible version");
    assert!(
        answer.trace().index_used.is_some(),
        "the read must walk the maintained FK index"
    );
    assert_eq!(
        answer.rows(),
        vec![vec![Value::Int64(7), Value::Text("current".into())]]
    );
    drop(reader);

    let writer = Database::open(&path).expect("reopen writer");
    writer
        .execute("DELETE FROM children WHERE id = 7", &HashMap::new())
        .expect("delete the updated row");
    writer.close().expect("close writer after delete");
    let reader = ReadSession::open(&path).expect("reopen file reader");
    assert_eq!(reader.route(), ReadRoute::File);
    let answer = reader
        .execute(
            "SELECT id FROM children WHERE parent_id = 1",
            &HashMap::new(),
        )
        .expect("retired FK posting does not name a deleted row");
    assert!(
        answer.trace().index_used.is_some(),
        "the empty result must still probe the maintained FK index"
    );
    assert!(answer.rows().is_empty());
}
