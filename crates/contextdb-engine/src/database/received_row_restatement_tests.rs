use super::*;
use crate::identity::FabricIdentity;
use std::sync::Arc;
use uuid::Uuid;

fn empty_params() -> HashMap<String, Value> {
    HashMap::new()
}

/// A table declared `SYNC CONFLICT KEEP FIRST`, exactly the declaration
/// syntax a fresh edge would install for structural identity rows it must
/// never let a later sync source overwrite.
fn keep_first_notes(db: &Database) {
    db.execute(
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP FIRST",
        &empty_params(),
    )
    .expect("declare keep-first notes");
}

fn insert(db: &Database, id: Uuid, body: &str) {
    db.execute(
        "INSERT INTO notes (id, body) VALUES ($id, $body)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("body".to_string(), Value::Text(body.to_string())),
        ]),
    )
    .expect("insert note");
}

fn note_body(db: &Database, id: Uuid) -> String {
    let result = db
        .execute(
            "SELECT body FROM notes WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(id))]),
        )
        .expect("query note");
    assert_eq!(result.rows.len(), 1, "exactly one note for this id");
    let column = result
        .columns
        .iter()
        .position(|name| name == "body")
        .expect("body column present");
    match &result.rows[0][column] {
        Value::Text(body) => body.clone(),
        other => panic!("unexpected body value: {other:?}"),
    }
}

fn identity(root: &tempfile::TempDir, name: &str) -> Arc<FabricIdentity> {
    Arc::new(
        FabricIdentity::load_or_generate(&root.path().join(format!("{name}.key")))
            .expect("persist test identity"),
    )
}

/// Sign the outbound lineage for every row in `changes`, exactly as the
/// authoring node would before handing that row to a hub — mirrors
/// `in_memory_lineage_tests.rs`'s identical helper.
fn lineages(
    db: &Database,
    changes: &ChangeSet,
    tenant: &TenantId,
    author: &Arc<FabricIdentity>,
    incarnation: Incarnation,
) -> HashMap<(String, Vec<u8>, Lsn), crate::protocol::WireRowLineage> {
    db.outbound_row_lineages(changes, tenant, &author.node_id(), incarnation, &|bytes| {
        Ok(author.sign_lineage(bytes))
    })
    .expect("committed rows retain authenticated lineage")
}

fn lineage_entries(
    changes: &ChangeSet,
    lineages: &HashMap<(String, Vec<u8>, Lsn), crate::protocol::WireRowLineage>,
) -> Vec<(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)> {
    changes
        .rows
        .iter()
        .map(|row| {
            let key = (
                row.table.clone(),
                rmp_serde::to_vec(&row.natural_key).expect("encode natural key"),
                row.lsn,
            );
            (
                row.table.clone(),
                row.natural_key.clone(),
                row.lsn,
                lineages.get(&key).cloned().expect("row lineage"),
            )
        })
        .collect()
}

/// The same outbound-row classification `SyncClient::pending_push_changes`
/// relies on (`checked_changes_since_with_arrivals` -> direction filter ->
/// `filter_outbound_received_ddl` -> `drop_rows_that_arrived_by_sync`),
/// driven directly against the store rather than through the network-facing
/// `SyncClient`, which additionally needs a live transport, tenant, and push
/// watermark this regression has no use for.
fn outbound_pending_rows(db: &Database) -> Vec<RowChange> {
    let (changes, _arrivals, provenance) = db.checked_changes_since_with_arrivals(Lsn(0)).unwrap();
    let history = db.sync_direction_history();
    let changes =
        changes.filter_by_direction_history(&history, &[SyncDirection::Push, SyncDirection::Both]);
    let changes = db
        .filter_outbound_received_ddl(changes, &provenance, None)
        .unwrap();
    crate::sync_client::drop_rows_that_arrived_by_sync(db, changes).rows
}

fn reopen(db: Database, path: &std::path::Path) -> Database {
    db.close().unwrap();
    drop(db);
    Database::open(path).unwrap()
}

/// Build the pulled batch a hub would serve for one row it holds under
/// `id`/`body`: a fresh in-memory "source" database plays the row's
/// original creator so the batch carries a real, verifiable lineage
/// attestation, exactly like a production pull.
fn pulled_row_batch(
    root: &tempfile::TempDir,
    tenant: &TenantId,
    id: Uuid,
    body: &str,
) -> (
    ChangeSet,
    Vec<(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)>,
) {
    let source = Database::open_memory();
    keep_first_notes(&source);
    let creator = identity(root, "creator");
    let source_incarnation = source
        .sync_incarnation(tenant)
        .expect("source database incarnation");
    let before = source.current_lsn();
    insert(&source, id, body);
    let changes = source.changes_since(before);
    assert_eq!(changes.rows.len(), 1, "one row per pulled batch");
    let signed = lineages(&source, &changes, tenant, &creator, source_incarnation);
    let entries = lineage_entries(&changes, &signed);
    (changes, entries)
}

#[test]
fn identical_pulled_row_clears_local_push_obligation_on_keep_first_table() {
    let root = tempfile::tempdir().unwrap();
    let tenant = TenantId::from("row-restatement-identical-tenant");
    let path = root.path().join("row-restatement-identical.redb");
    let db = Database::open(&path).unwrap();
    keep_first_notes(&db);

    let id = Uuid::new_v4();
    insert(&db, id, "original");

    let (changes, entries) = pulled_row_batch(&root, &tenant, id, "original");
    let result = db
        .apply_authenticated_received_changes_with_lineages(
            changes,
            &HashMap::new(),
            SyncAdoption::Continuing,
            None,
            &tenant,
            &entries,
            None,
            false,
        )
        .expect("an authoritative restatement of an identical row must apply cleanly");

    assert_eq!(
        result.skipped_rows, 0,
        "an incoming row that exactly restates the local row is agreement, not a refusal"
    );
    assert!(
        result.conflicts.iter().all(|conflict| conflict.natural_key
            != NaturalKey::single("id".to_string(), Value::Uuid(id))),
        "an identical restatement must not be reported as a keep_first conflict: {:?}",
        result.conflicts
    );
    assert_eq!(
        note_body(&db, id),
        "original",
        "the row's value is unchanged by an identical restatement"
    );

    assert_eq!(
        outbound_pending_rows(&db),
        Vec::new(),
        "an authoritative pull of a byte-identical row must clear the local push obligation"
    );

    let db = reopen(db, &path);
    assert_eq!(
        outbound_pending_rows(&db),
        Vec::new(),
        "the cleared push obligation must survive close+reopen"
    );
}

#[test]
fn divergent_pulled_row_keeps_current_keep_first_behavior() {
    let root = tempfile::tempdir().unwrap();
    let tenant = TenantId::from("row-restatement-divergent-tenant");
    let path = root.path().join("row-restatement-divergent.redb");
    let db = Database::open(&path).unwrap();
    keep_first_notes(&db);

    let id = Uuid::new_v4();
    insert(&db, id, "original");

    let (changes, entries) = pulled_row_batch(&root, &tenant, id, "divergent");
    let result = db
        .apply_authenticated_received_changes_with_lineages(
            changes,
            &HashMap::new(),
            SyncAdoption::Continuing,
            None,
            &tenant,
            &entries,
            None,
            false,
        )
        .expect("a divergent incoming row under KEEP FIRST is a handled refusal, not an error");

    // Pinning today's exact KEEP FIRST semantics (database.rs's
    // `(Some(local), ConflictPolicy::InsertIfNotExists)` arm): the incoming
    // row is skipped and reported as a keep_first conflict, and the local
    // value is untouched.
    assert_eq!(
        result.skipped_rows, 1,
        "a genuinely divergent incoming row is still refused under KEEP FIRST"
    );
    assert_eq!(
        result.conflicts.len(),
        1,
        "a genuinely divergent incoming row still reports exactly one keep_first conflict"
    );
    assert_eq!(
        result.conflicts[0].reason.as_deref(),
        Some("keep_first"),
        "the conflict reason for a divergent row stays keep_first"
    );
    assert_eq!(
        note_body(&db, id),
        "original",
        "KEEP FIRST must keep the local value against a divergent incoming row"
    );

    let assert_local_row_stays_pending = |db: &Database, when: &str| {
        let outbound = outbound_pending_rows(db);
        assert_eq!(
            outbound.len(),
            1,
            "the local row must remain push-eligible against a divergent incoming row {when}, got {outbound:?}"
        );
        assert_eq!(
            outbound[0].natural_key,
            NaturalKey::single("id".to_string(), Value::Uuid(id)),
            "the surviving pending row must be the local note {when}"
        );
    };

    assert_local_row_stays_pending(&db, "immediately after apply");
    let db = reopen(db, &path);
    assert_local_row_stays_pending(&db, "after reopen");
}
