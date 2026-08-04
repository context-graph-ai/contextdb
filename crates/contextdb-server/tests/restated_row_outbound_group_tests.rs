//! A row and its vectors leave as ONE outbound group.
//!
//! When an incoming row exactly restates what this node already holds, the apply
//! records sync provenance for the held row without rewriting it. That sidecar
//! describes the row's CURRENT version, so it can only classify an outbound
//! change that carries that same version.
//!
//! A later commit can republish a surviving row — an applied authoritative purge
//! does exactly this — producing a change at a position the stored row version
//! never took. Classifying that change by the sidecar drops the ROW while the
//! vector half of its group, which has always demanded the version match, still
//! rides. The peer then receives a vector whose owning row never arrived and
//! fails the apply with `not found: row <id> in table <name>`.
//!
//! Discipline: no sleeps, no clock reads. Both halves of the group are asserted
//! on the same constructed changes, deterministically.

use contextdb_core::{Lsn, Value, VectorIndexRef};
use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy};
use contextdb_engine::{ChangeSet, Database, NaturalKey, RowChange, SyncAdoption, VectorChange};
use std::collections::HashMap;
use uuid::Uuid;

fn open_notes(path: &std::path::Path) -> Database {
    let db = Database::open(path).expect("open notes database");
    db.execute(
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT, embedding VECTOR(3)) \
         SYNC CONFLICT KEEP LATEST",
        &HashMap::new(),
    )
    .expect("create notes table");
    db
}

fn insert_note(db: &Database, id: Uuid, body: &str) {
    db.execute(
        "INSERT INTO notes (id, body, embedding) VALUES ($id, $body, '[1,0,0]')",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("body".to_string(), Value::Text(body.to_string())),
        ]),
    )
    .expect("insert note with its vector");
}

/// Serve back exactly the row this node already holds, the shape a hub produces
/// when an edge pulls rows it authored itself.
fn apply_identical_row(db: &Database, id: Uuid, body: &str) {
    let source_lsn = Lsn(1);
    let changes = ChangeSet {
        rows: vec![RowChange {
            table: "notes".to_string(),
            natural_key: NaturalKey::single("id".to_string(), Value::Uuid(id)),
            values: HashMap::from([
                ("id".to_string(), Value::Uuid(id)),
                ("body".to_string(), Value::Text(body.to_string())),
            ]),
            deleted: false,
            lsn: source_lsn,
            created_at: None,
        }],
        ..ChangeSet::default()
    };
    db.apply_synced_changes(
        changes,
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        &HashMap::from([(source_lsn, None)]),
        SyncAdoption::Continuing,
    )
    .expect("an identical restatement applies as agreement");
}

#[test]
fn a_republished_restated_row_stays_grouped_with_its_vector() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let db = open_notes(&temp.path().join("restated-group.redb"));

    let id = Uuid::new_v4();
    insert_note(&db, id, "held-value");
    let natural_key = NaturalKey::single("id".to_string(), Value::Uuid(id));

    let outbound = db.changes_since(Lsn(0));
    let stored = outbound
        .rows
        .iter()
        .find(|row| row.natural_key == natural_key)
        .expect("the authored row is outbound before any restatement")
        .clone();
    let stored_vector = outbound
        .vectors
        .iter()
        .find(|vector| vector.lsn == stored.lsn)
        .expect("the authored row's vector shares its commit")
        .clone();

    apply_identical_row(&db, id, "held-value");
    assert!(
        db.row_version_arrived_by_sync("notes", &natural_key),
        "an identical authoritative restatement records arrival provenance for the held row"
    );

    // The stored version itself: both halves agree it arrived, so the whole
    // group is withheld and the push obligation is discharged.
    assert!(
        db.row_change_arrived_by_sync(&stored),
        "the change describing the stored version is classified by its own sidecar"
    );
    assert!(
        db.vector_change_arrived_by_sync(&stored_vector),
        "the vector of that same version is classified identically"
    );

    // A republication of the surviving row at a later position, as an applied
    // purge produces. The stored version is unchanged, so neither half of the
    // group may claim this change arrived by sync.
    let republished_lsn = Lsn(stored.lsn.0 + 8);
    let republished_row = RowChange {
        lsn: republished_lsn,
        ..stored.clone()
    };
    let republished_vector = VectorChange {
        index: VectorIndexRef::new("notes", "embedding"),
        row_id: stored_vector.row_id,
        vector: stored_vector.vector.clone(),
        lsn: republished_lsn,
    };

    let row_withheld = db.row_change_arrived_by_sync(&republished_row);
    let vector_withheld = db.vector_change_arrived_by_sync(&republished_vector);
    assert!(
        !row_withheld,
        "a republished row at a position the stored version never took must not be \
         classified as arrived by sync"
    );
    assert_eq!(
        row_withheld, vector_withheld,
        "a row and its vector must be classified as ONE group; withholding the row \
         ({row_withheld}) while the vector rides ({vector_withheld}) strands that \
         vector at a peer that never receives its owning row"
    );
}

#[test]
fn a_restated_row_that_is_then_locally_edited_returns_to_outbound_work() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let db = open_notes(&temp.path().join("restated-then-edited.redb"));

    let id = Uuid::new_v4();
    insert_note(&db, id, "held-value");
    let natural_key = NaturalKey::single("id".to_string(), Value::Uuid(id));
    apply_identical_row(&db, id, "held-value");
    assert!(
        db.row_version_arrived_by_sync("notes", &natural_key),
        "the restatement marks the held version"
    );

    db.execute(
        "UPDATE notes SET body = $body WHERE id = $id",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("body".to_string(), Value::Text("local-edit".to_string())),
        ]),
    )
    .expect("edit the restated row locally");

    let edited = db
        .changes_since(Lsn(0))
        .rows
        .into_iter()
        .rfind(|row| row.natural_key == natural_key)
        .expect("the local edit is outbound");
    assert!(
        !db.row_change_arrived_by_sync(&edited),
        "local work after a restatement is this node's own and must still propagate"
    );
}
