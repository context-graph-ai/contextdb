use super::*;
use uuid::Uuid;

/// The receiving shape a consumer installs: a table whose `INCLUDING SYNC`
/// trigger writes derived rows inside the applying transaction, plus an
/// unrelated table carrying a vector column. Both travel in one sync unit.
fn declare_trigger_and_vector_schema(db: &Database) {
    db.execute(
        "CREATE TABLE outcomes (id UUID PRIMARY KEY, body TEXT)",
        &HashMap::new(),
    )
    .expect("declare outcomes");
    db.execute(
        "CREATE TABLE conflicts (id UUID PRIMARY KEY, outcome_id UUID, note TEXT)",
        &HashMap::new(),
    )
    .expect("declare conflicts");
    db.execute(
        "CREATE TABLE digests (id UUID PRIMARY KEY, body TEXT, vector_text VECTOR(3))",
        &HashMap::new(),
    )
    .expect("declare digests");
}

fn declare_including_sync_trigger(db: &Database) {
    db.execute(
        "CREATE TRIGGER conflict_detection_on_outcomes ON outcomes WHEN INSERT INCLUDING SYNC",
        &HashMap::new(),
    )
    .expect("declare the opted-in sync trigger");
}

/// The callback writes a derived row into the SAME transaction the sync apply
/// is using — the sanctioned in-transaction callback path.
fn register_deriving_callback(db: &Database) {
    db.register_trigger_callback("conflict_detection_on_outcomes", |handle, ctx| {
        let outcome_id = ctx
            .row_values
            .get("id")
            .and_then(Value::as_uuid)
            .copied()
            .ok_or_else(|| Error::Other("trigger row carries no outcome id".into()))?;
        handle.execute(
            "INSERT INTO conflicts (id, outcome_id, note) VALUES ($id, $outcome_id, $note)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("outcome_id".to_string(), Value::Uuid(outcome_id)),
                ("note".to_string(), Value::Text("derived".to_string())),
            ]),
        )?;
        Ok(())
    })
    .expect("register the trigger callback");
}

fn open_receiver(path: &std::path::Path) -> Database {
    let db = Database::open(path).expect("open receiver");
    declare_trigger_and_vector_schema(&db);
    declare_including_sync_trigger(&db);
    register_deriving_callback(&db);
    db.complete_initialization()
        .expect("receiver completes initialization");
    db
}

/// Author the incoming unit on a separate node so row ids, LSNs and the
/// vector's owning row group are all linked exactly as a real sync payload is.
fn incoming_unit(with_outcome: bool) -> (ChangeSet, Uuid, Uuid) {
    let source = Database::open_memory();
    declare_trigger_and_vector_schema(&source);
    let before = source.current_lsn();
    let digest_id = Uuid::new_v4();
    let outcome_id = Uuid::new_v4();
    // ONE source transaction, so both rows and the vector share a single
    // source LSN and therefore land in ONE applying transaction on the
    // receiver — the shape where the callback's writes and the vector's
    // recorded schema epoch meet.
    let tx = source.begin().expect("begin source transaction");
    source
        .execute_in_tx(
            tx,
            "INSERT INTO digests (id, body, vector_text) VALUES ($id, $body, '[1,0,0]')",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(digest_id)),
                ("body".to_string(), Value::Text("digest-body".to_string())),
            ]),
        )
        .expect("author the vector-bearing digest");
    if with_outcome {
        source
            .execute_in_tx(
                tx,
                "INSERT INTO outcomes (id, body) VALUES ($id, $body)",
                &HashMap::from([
                    ("id".to_string(), Value::Uuid(outcome_id)),
                    ("body".to_string(), Value::Text("outcome-body".to_string())),
                ]),
            )
            .expect("author the trigger-bearing outcome");
    }
    source.commit(tx).expect("commit the source transaction");
    (source.changes_since(before), digest_id, outcome_id)
}

fn live_vector_count(db: &Database) -> usize {
    db.execute(
        "SELECT id FROM digests WHERE vector_text IS NOT NULL",
        &HashMap::new(),
    )
    .expect("query digest vectors")
    .rows
    .len()
}

fn row_count(db: &Database, table: &str) -> usize {
    db.execute(&format!("SELECT id FROM {table}"), &HashMap::new())
        .unwrap_or_else(|err| panic!("query {table}: {err}"))
        .rows
        .len()
}

/// A sync unit may carry BOTH a row whose `INCLUDING SYNC` trigger derives
/// more rows inside the applying transaction AND a vector for an unrelated
/// table. The callback's writes are part of the same transaction, so they must
/// not invalidate the vector-schema epochs that transaction recorded — nothing
/// about the vector index changed. Before this was fixed the apply failed with
/// `vector index digests.vector_text changed while transaction was open`, so a
/// consumer that declared any deriving sync trigger could not receive vectors
/// at all.
#[test]
fn sync_apply_commits_a_vector_beside_a_deriving_including_sync_trigger() {
    let temp = tempfile::TempDir::new().unwrap();
    let db = open_receiver(&temp.path().join("trigger-vector-epoch.redb"));

    let (changes, digest_id, outcome_id) = incoming_unit(true);
    db.apply_synced_changes(
        changes,
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        &HashMap::new(),
        SyncAdoption::Continuing,
    )
    .expect("a unit carrying a deriving sync trigger and a vector must commit");

    assert_eq!(
        row_count(&db, "digests"),
        1,
        "the vector's owning row lands"
    );
    assert_eq!(
        row_count(&db, "outcomes"),
        1,
        "the trigger's source row lands"
    );
    assert_eq!(
        row_count(&db, "conflicts"),
        1,
        "the callback's derived row lands in the same transaction"
    );
    assert_eq!(
        live_vector_count(&db),
        1,
        "the vector itself lands: digest {digest_id}, outcome {outcome_id}"
    );
}

/// The single-ingredient control. The identical unit WITHOUT the trigger-bearing
/// row commits today, which is what proves the deriving callback — not the
/// vector — is what the epoch bookkeeping mishandles.
#[test]
fn the_same_unit_without_the_trigger_row_commits() {
    let temp = tempfile::TempDir::new().unwrap();
    let db = open_receiver(&temp.path().join("trigger-vector-epoch-control.redb"));

    let (changes, _digest_id, _outcome_id) = incoming_unit(false);
    db.apply_synced_changes(
        changes,
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        &HashMap::new(),
        SyncAdoption::Continuing,
    )
    .expect("the vector-only unit commits");

    assert_eq!(
        row_count(&db, "digests"),
        1,
        "the vector's owning row lands"
    );
    assert_eq!(
        row_count(&db, "conflicts"),
        0,
        "no trigger row arrived, so nothing was derived"
    );
    assert_eq!(
        live_vector_count(&db),
        1,
        "the vector lands when no deriving trigger row shares its unit"
    );
}
