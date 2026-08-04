use super::*;
use crate::identity::FabricIdentity;
use std::sync::Arc;
use uuid::Uuid;

/// A connected schema: the version marker a fresh edge writes for itself before
/// it ever reaches a hub, plus the records that reference it. Both legs are
/// declared `SYNC TWO WAY`, so each carries a pull leg, and both are
/// `KEEP FIRST` — the shape a structural identity row takes.
fn declare_connected_keep_first_schema(db: &Database) {
    db.execute(
        "CREATE TABLE schema_version (id UUID PRIMARY KEY, version TEXT) \
         SYNC TWO WAY SYNC CONFLICT KEEP FIRST",
        &HashMap::new(),
    )
    .expect("declare schema_version");
    db.execute(
        "CREATE TABLE records (id UUID PRIMARY KEY, schema_id UUID REFERENCES schema_version(id), body TEXT) \
         SYNC TWO WAY SYNC CONFLICT KEEP FIRST",
        &HashMap::new(),
    )
    .expect("declare records");
}

fn insert_schema_version(db: &Database, id: Uuid, version: &str) {
    db.execute(
        "INSERT INTO schema_version (id, version) VALUES ($id, $version)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("version".to_string(), Value::Text(version.to_string())),
        ]),
    )
    .expect("insert schema version");
}

fn insert_record(db: &Database, id: Uuid, schema_id: Uuid, body: &str) {
    db.execute(
        "INSERT INTO records (id, schema_id, body) VALUES ($id, $schema_id, $body)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("schema_id".to_string(), Value::Uuid(schema_id)),
            ("body".to_string(), Value::Text(body.to_string())),
        ]),
    )
    .expect("insert record");
}

fn schema_version_value(db: &Database, id: Uuid) -> Option<String> {
    let result = db
        .execute(
            "SELECT version FROM schema_version WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(id))]),
        )
        .expect("query schema version");
    let column = result
        .columns
        .iter()
        .position(|name| name == "version")
        .expect("version column present");
    result.rows.first().map(|row| match &row[column] {
        Value::Text(version) => version.clone(),
        other => panic!("unexpected version value: {other:?}"),
    })
}

fn record_count(db: &Database) -> usize {
    db.execute("SELECT id FROM records", &HashMap::new())
        .expect("query records")
        .rows
        .len()
}

fn identity(root: &tempfile::TempDir, name: &str) -> Arc<FabricIdentity> {
    Arc::new(
        FabricIdentity::load_or_generate(&root.path().join(format!("{name}.key")))
            .expect("persist test identity"),
    )
}

fn lineage_entries(
    db: &Database,
    changes: &ChangeSet,
    tenant: &TenantId,
    author: &Arc<FabricIdentity>,
    incarnation: Incarnation,
) -> Vec<(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)> {
    let signed = db
        .outbound_row_lineages(changes, tenant, &author.node_id(), incarnation, &|bytes| {
            Ok(author.sign_lineage(bytes))
        })
        .expect("committed rows retain authenticated lineage");
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
                signed.get(&key).cloned().expect("row lineage"),
            )
        })
        .collect()
}

/// The hub's authoritative connected history: its own first-accepted schema
/// version plus sixteen records that reference it, authored on a separate node
/// so the batch carries real lineage attestation exactly like a production pull.
fn hub_connected_history(
    root: &tempfile::TempDir,
    tenant: &TenantId,
    schema_id: Uuid,
    record_ids: &[Uuid],
) -> (
    ChangeSet,
    Vec<(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)>,
) {
    let hub = Database::open_memory();
    declare_connected_keep_first_schema(&hub);
    let author = identity(root, "hub-author");
    let incarnation = hub.sync_incarnation(tenant).expect("hub incarnation");
    let before = hub.current_lsn();
    insert_schema_version(&hub, schema_id, "hub-accepted-version");
    for (index, id) in record_ids.iter().enumerate() {
        insert_record(&hub, *id, schema_id, &format!("hub-record-{index}"));
    }
    let changes = hub.changes_since(before);
    assert_eq!(
        changes.rows.len(),
        record_ids.len() + 1,
        "the hub unit carries its schema row plus every record"
    );
    let entries = lineage_entries(&hub, &changes, tenant, &author, incarnation);
    (changes, entries)
}

/// A dependency-complete pull unit is the hub's authoritative connected
/// content. When one member is a KEEP FIRST row the edge also wrote locally,
/// that is a policy resolution — the edge adopts the hub-accepted value and the
/// losing local value is reported — never a failure of the whole unit. Before
/// this was fixed, a receipt-less dependency-complete unit carrying a single
/// resolved row was rejected wholesale, so an edge holding any divergent
/// structural row could not restore at all.
#[test]
fn dependency_complete_pull_unit_adopts_hub_keep_first_value_and_applies_siblings() {
    let root = tempfile::tempdir().unwrap();
    let tenant = TenantId::from("dependency-unit-pull-adoption");
    let path = root.path().join("edge.redb");
    let edge = Database::open(&path).unwrap();
    declare_connected_keep_first_schema(&edge);

    // The edge wrote its own version marker before ever reaching the hub.
    let schema_id = Uuid::new_v4();
    insert_schema_version(&edge, schema_id, "edge-local-version");
    let record_ids = (0..16).map(|_| Uuid::new_v4()).collect::<Vec<_>>();

    let (changes, entries) = hub_connected_history(&root, &tenant, schema_id, &record_ids);
    let result = edge
        .apply_authenticated_received_changes_with_lineages(
            changes,
            &HashMap::new(),
            SyncAdoption::Continuing,
            None,
            &tenant,
            &entries,
            None,
            true,
        )
        .expect("a dependency-complete pull unit must apply despite a policy-resolved member");

    assert_eq!(
        schema_version_value(&edge, schema_id).as_deref(),
        Some("hub-accepted-version"),
        "the edge adopts the hub-accepted value for the KEEP FIRST row it disagreed on"
    );
    assert_eq!(
        record_count(&edge),
        record_ids.len(),
        "every sibling record in the unit lands"
    );
    assert_eq!(
        result.applied_rows,
        record_ids.len() + 1,
        "the adopted row and its siblings all count as applied work"
    );

    let resolution = result
        .conflicts
        .iter()
        .find(|conflict| {
            conflict.natural_key == NaturalKey::single("id".to_string(), Value::Uuid(schema_id))
        })
        .expect("the resolved row is reported as a typed conflict diagnostic, never silently");
    assert_eq!(
        resolution.table.as_deref(),
        Some("schema_version"),
        "the diagnostic names the table"
    );
    assert_eq!(
        resolution.resolution,
        ConflictPolicy::InsertIfNotExists,
        "the diagnostic names the policy that resolved it"
    );
    assert!(
        resolution.winning_author_node_id.is_some(),
        "the diagnostic names the winning author: {resolution:?}"
    );
    assert!(
        result.conflicts.iter().all(|conflict| {
            conflict.natural_key == NaturalKey::single("id".to_string(), Value::Uuid(schema_id))
        }),
        "only the row that actually disagreed is reported: {:?}",
        result.conflicts
    );
}

/// The same unit shape with nothing to resolve stays exactly as it was: every
/// member applies and no diagnostic is produced.
#[test]
fn dependency_complete_pull_unit_without_disagreement_reports_no_conflict() {
    let root = tempfile::tempdir().unwrap();
    let tenant = TenantId::from("dependency-unit-pull-agreement");
    let path = root.path().join("edge-agreeing.redb");
    let edge = Database::open(&path).unwrap();
    declare_connected_keep_first_schema(&edge);

    let schema_id = Uuid::new_v4();
    let record_ids = (0..16).map(|_| Uuid::new_v4()).collect::<Vec<_>>();
    let (changes, entries) = hub_connected_history(&root, &tenant, schema_id, &record_ids);
    let result = edge
        .apply_authenticated_received_changes_with_lineages(
            changes,
            &HashMap::new(),
            SyncAdoption::Continuing,
            None,
            &tenant,
            &entries,
            None,
            true,
        )
        .expect("an uncontested dependency-complete pull unit applies");

    assert_eq!(
        schema_version_value(&edge, schema_id).as_deref(),
        Some("hub-accepted-version"),
        "the hub's schema row lands on a fresh edge"
    );
    assert_eq!(record_count(&edge), record_ids.len(), "every record lands");
    assert!(
        result.conflicts.is_empty(),
        "nothing disagreed, so nothing is reported: {:?}",
        result.conflicts
    );
    assert_eq!(result.skipped_rows, 0, "nothing was turned away");
}

/// A push the hub cannot receipt is still a PUSH. An unidentified peer reaches
/// `spawn_apply_and_reply`'s receipt-less branch (sync_server.rs) carrying real
/// rows, so if an absent receipt were read as "this is a pull" that peer could
/// overwrite the hub's first-accepted value on a KEEP FIRST table — inverting
/// the one thing KEEP FIRST protects. The hub keeps its value and refuses.
#[test]
fn keyless_push_cannot_overwrite_the_hub_first_accepted_keep_first_value() {
    let root = tempfile::tempdir().unwrap();
    let tenant = TenantId::from("keyless-push-keep-first");
    let path = root.path().join("hub.redb");
    let hub = Database::open(&path).unwrap();
    declare_connected_keep_first_schema(&hub);

    let schema_id = Uuid::new_v4();
    insert_schema_version(&hub, schema_id, "hub-first-accepted");

    // The pushed batch, authored on another node so it carries real lineage,
    // exactly as an unidentified peer's push arrives.
    let pusher = Database::open_memory();
    declare_connected_keep_first_schema(&pusher);
    let author = identity(&root, "keyless-pusher");
    let incarnation = pusher
        .sync_incarnation(&tenant)
        .expect("pusher incarnation");
    let before = pusher.current_lsn();
    insert_schema_version(&pusher, schema_id, "pushed-divergent-value");
    let changes = pusher.changes_since(before);
    let entries = lineage_entries(&pusher, &changes, &tenant, &author, incarnation);

    let result = hub
        .apply_authenticated_received_changes_with_lineages_as_hub_push(
            changes,
            &HashMap::new(),
            SyncAdoption::Continuing,
            None,
            &tenant,
            &entries,
            None,
            false,
        )
        .expect("an unreceiptable push is a handled refusal, not an error");

    assert_eq!(
        schema_version_value(&hub, schema_id).as_deref(),
        Some("hub-first-accepted"),
        "the hub keeps the value it accepted first; an unidentified pusher cannot displace it"
    );
    assert_eq!(
        result.applied_rows, 0,
        "nothing was written: {:?}",
        result.conflicts
    );
    assert_eq!(
        result.skipped_rows, 1,
        "the pushed row is refused and counted, not silently dropped"
    );
    let conflict = result
        .conflicts
        .iter()
        .find(|conflict| {
            conflict.natural_key == NaturalKey::single("id".to_string(), Value::Uuid(schema_id))
        })
        .expect("the refusal is reported as a typed diagnostic");
    assert_eq!(
        conflict.reason.as_deref(),
        Some("keep_first"),
        "a refused push reports the policy that kept the local value, never hub adoption"
    );
}
