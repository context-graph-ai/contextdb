#![cfg(feature = "sync-orchestration")]

use super::*;
use crate::identity::FabricIdentity;
use crate::sync_types::SyncAdoption;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

fn notes(db: &Database) {
    db.execute(
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP LATEST",
        &HashMap::new(),
    )
    .expect("declare notes");
}

fn keep_first_notes(db: &Database) {
    db.execute(
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP FIRST",
        &HashMap::new(),
    )
    .expect("declare keep-first notes");
}

fn server_wins_work_claims(db: &Database) {
    crate::work_ledger::install_work_ledger_schema(db).expect("declare server-wins work claims");
}

fn insert_work_claim(db: &Database, claim_key: &str) {
    db.execute(
        "INSERT INTO work_claims (claim_key, job_id, attempt, node_id, lease_deadline, claimed_at) \
         VALUES ($claim_key, $job_id, $attempt, $node_id, $lease_deadline, $claimed_at)",
        &HashMap::from([
            ("claim_key".to_string(), Value::Text(claim_key.to_string())),
            ("job_id".to_string(), Value::Text("job".to_string())),
            ("attempt".to_string(), Value::Int64(1)),
            ("node_id".to_string(), Value::Text("worker".to_string())),
            ("lease_deadline".to_string(), Value::Timestamp(2)),
            ("claimed_at".to_string(), Value::Timestamp(1)),
        ]),
    )
    .expect("insert server-wins work claim");
}

fn work_claim_is_absent(db: &Database, claim_key: &str) -> bool {
    db.execute(
        "SELECT claim_key FROM work_claims WHERE claim_key = $claim_key",
        &HashMap::from([("claim_key".to_string(), Value::Text(claim_key.to_string()))]),
    )
    .expect("query work claim")
    .rows
    .is_empty()
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

fn update(db: &Database, id: Uuid, body: &str) {
    db.execute(
        "UPDATE notes SET body = $body WHERE id = $id",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("body".to_string(), Value::Text(body.to_string())),
        ]),
    )
    .expect("update note");
}

fn delete(db: &Database, id: Uuid) {
    db.execute(
        "DELETE FROM notes WHERE id = $id",
        &HashMap::from([("id".to_string(), Value::Uuid(id))]),
    )
    .expect("delete note");
}

fn note_is_absent(db: &Database, id: Uuid) -> bool {
    db.execute(
        "SELECT id FROM notes WHERE id = $id",
        &HashMap::from([("id".to_string(), Value::Uuid(id))]),
    )
    .expect("query note")
    .rows
    .is_empty()
}

fn identity(root: &tempfile::TempDir, name: &str) -> Arc<FabricIdentity> {
    Arc::new(
        FabricIdentity::load_or_generate(&root.path().join(format!("{name}.key")))
            .expect("persist test identity"),
    )
}

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
    .expect("committed in-memory rows retain authenticated lineage")
}

fn only_lineage(
    lineages: &HashMap<(String, Vec<u8>, Lsn), crate::protocol::WireRowLineage>,
) -> crate::protocol::WireRowLineage {
    assert_eq!(lineages.len(), 1, "one row has one lineage");
    lineages.values().next().cloned().expect("one lineage")
}

fn deleted_lineage(
    changes: &ChangeSet,
    lineages: &HashMap<(String, Vec<u8>, Lsn), crate::protocol::WireRowLineage>,
) -> crate::protocol::WireRowLineage {
    let row = changes
        .rows
        .iter()
        .find(|row| row.deleted)
        .expect("one delete row");
    lineages
        .get(&(
            row.table.clone(),
            rmp_serde::to_vec(&row.natural_key).expect("encode natural key"),
            row.lsn,
        ))
        .cloned()
        .expect("delete lineage")
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

#[test]
fn committed_memory_creation_binds_once_across_retry_and_update() {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = TenantId::from("memory-lineage");
    let db = Arc::new(Database::open_memory());
    notes(&db);
    let author = identity(&root, "author");
    let other_author = identity(&root, "other-author");
    let incarnation = db.sync_incarnation(&tenant).expect("memory incarnation");
    let base = db.current_lsn();
    let id = Uuid::new_v4();
    insert(&db, id, "created");
    let created = db.changes_since(base);

    let first = lineages(&db, &created, &tenant, &author, incarnation);
    let retry = lineages(&db, &created, &tenant, &author, incarnation);
    assert_eq!(retry, first, "retry keeps the signed bytes unchanged");
    let created_lineage = only_lineage(&first);

    let after_insert = db.current_lsn();
    update(&db, id, "updated");
    let updated = db.changes_since(after_insert);
    let updated_lineage = only_lineage(&lineages(&db, &updated, &tenant, &author, incarnation));
    assert_eq!(
        updated_lineage, created_lineage,
        "updates keep the creation tuple and signature"
    );

    assert!(
        db.outbound_row_lineages(
            &created,
            &tenant,
            &other_author.node_id(),
            incarnation,
            &|bytes| Ok(other_author.sign_lineage(bytes)),
        )
        .is_err(),
        "one in-memory database life refuses a different explicit identity"
    );
}

#[test]
fn file_creation_lineage_survives_state_propagation_before_first_sync() {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = TenantId::from("file-propagation-lineage");
    let db = Database::open(root.path().join("edge.db")).expect("open file database");
    db.execute(
        "CREATE TABLE parents (id UUID PRIMARY KEY, status TEXT STATE_MACHINE(status: active -> archived)) SYNC CONFLICT KEEP LATEST",
        &HashMap::new(),
    )
    .expect("declare parents");
    db.execute(
        "CREATE TABLE children (id UUID PRIMARY KEY, parent_id UUID REFERENCES parents(id) ON STATE archived PROPAGATE SET archived, status TEXT STATE_MACHINE(status: active -> archived)) SYNC CONFLICT KEEP LATEST",
        &HashMap::new(),
    )
    .expect("declare children");
    let parent_id = Uuid::new_v4();
    let child_id = Uuid::new_v4();
    db.execute(
        "INSERT INTO parents (id, status) VALUES ($id, 'active')",
        &HashMap::from([("id".to_string(), Value::Uuid(parent_id))]),
    )
    .expect("insert parent");
    db.execute(
        "INSERT INTO children (id, parent_id, status) VALUES ($id, $parent_id, 'active')",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(child_id)),
            ("parent_id".to_string(), Value::Uuid(parent_id)),
        ]),
    )
    .expect("insert child");
    let child_creation_lsn = db.current_lsn();
    db.execute(
        "UPDATE parents SET status = 'archived' WHERE id = $id",
        &HashMap::from([("id".to_string(), Value::Uuid(parent_id))]),
    )
    .expect("archive parent and propagate child state");
    let propagation_lsn = db.current_lsn();
    db.close().expect("close before first outbound binding");
    drop(db);
    let db = Database::open(root.path().join("edge.db")).expect("reopen file database");

    let author = identity(&root, "author");
    let incarnation = db.sync_incarnation(&tenant).expect("file incarnation");
    let changes = db.changes_since(Lsn(0));
    let creation_evidence = db
        .persistence
        .as_ref()
        .expect("file database has persistence")
        .load_config_values_with_prefix::<DurableUnboundCreationLineage>(
            "sync_creation_lineage.v1.",
        )
        .expect("read creation evidence");
    assert_eq!(
        creation_evidence.len(),
        2,
        "parent and child creation evidence must survive propagation: {creation_evidence:?}"
    );
    for (_, evidence) in &creation_evidence {
        let live_row_id = db
            .row_id_for_natural_key_full(
                &evidence.table,
                &evidence.natural_key,
                db.snapshot_for_read(),
            )
            .expect("created row remains visible");
        assert_eq!(
            live_row_id, evidence.local_row_id,
            "propagation must keep the local row pinned to its creation evidence: {evidence:?}"
        );
    }
    let authenticated = lineages(&db, &changes, &tenant, &author, incarnation);
    assert_eq!(
        authenticated.len(),
        changes.rows.len(),
        "every final propagated row keeps its committed creation lineage"
    );
    let child_change = changes
        .rows
        .iter()
        .filter(|row| row.table == "children" && !row.deleted)
        .max_by_key(|row| row.lsn)
        .expect("final child change exists");
    assert_eq!(
        child_change.lsn, propagation_lsn,
        "the child value belongs to the later propagation commit"
    );
    let child_lineage = authenticated
        .get(&(
            child_change.table.clone(),
            rmp_serde::to_vec(&child_change.natural_key).expect("encode child key"),
            child_change.lsn,
        ))
        .expect("child lineage exists");
    assert_eq!(
        child_lineage.author_local_mutation_position, child_creation_lsn,
        "propagation preserves the child's original creation position"
    );
}

#[test]
fn rolled_back_memory_creation_publishes_no_lineage_state() {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = TenantId::from("memory-rollback-lineage");
    let db = Database::open_memory();
    notes(&db);
    let author = identity(&root, "author");
    let incarnation = db.sync_incarnation(&tenant).expect("memory incarnation");
    let base = db.current_lsn();
    let id = Uuid::new_v4();
    let tx = db.begin().expect("begin");
    db.execute_in_tx(
        tx,
        "INSERT INTO notes (id, body) VALUES ($id, $body)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("body".to_string(), Value::Text("rolled back".to_string())),
        ]),
    )
    .expect("stage insert");
    db.rollback(tx).expect("rollback");
    assert!(
        db.changes_since(base).rows.is_empty(),
        "rollback publishes no row"
    );

    insert(&db, id, "committed");
    let committed = db.changes_since(base);
    let lineage = only_lineage(&lineages(&db, &committed, &tenant, &author, incarnation));
    assert_eq!(
        lineage.author_local_mutation_position, committed.rows[0].lsn,
        "the creation tuple comes from the committed insert, never the rolled-back one"
    );

    let contested = Uuid::new_v4();
    let winner = db.begin().expect("begin winner");
    let loser = db.begin().expect("begin loser");
    for tx in [winner, loser] {
        db.execute_in_tx(
            tx,
            "INSERT INTO notes (id, body) VALUES ($id, $body)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(contested)),
                ("body".to_string(), Value::Text("contested".to_string())),
            ]),
        )
        .expect("stage contested insert");
    }
    db.commit(winner).expect("commit winner");
    assert!(db.commit(loser).is_err(), "duplicate commit is refused");
    let _ = db.rollback(loser);
    let published = db.changes_since(base);
    let published_lineages = lineages(&db, &published, &tenant, &author, incarnation);
    assert_eq!(
        published_lineages.len(),
        published.rows.len(),
        "a failed commit publishes neither a row nor lineage state"
    );
}

#[test]
fn received_memory_rows_keep_creator_lineage_through_relay_and_schema_apply() {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = TenantId::from("memory-relay-lineage");
    let author = identity(&root, "author");
    let relay_author = identity(&root, "relay");
    let source = Database::open_memory();
    notes(&source);
    let source_base = source.current_lsn();
    let id = Uuid::new_v4();
    insert(&source, id, "source");
    let changes = source.changes_since(source_base);
    let source_incarnation = source
        .sync_incarnation(&tenant)
        .expect("source incarnation");
    let source_lineages = lineages(&source, &changes, &tenant, &author, source_incarnation);
    let creator = only_lineage(&source_lineages);

    let relay = Database::open_memory();
    notes(&relay);
    relay
        .apply_authenticated_received_changes_with_lineages(
            changes.clone(),
            &HashMap::new(),
            SyncAdoption::Continuing,
            None,
            &tenant,
            &lineage_entries(&changes, &source_lineages),
            None,
            false,
        )
        .expect("relay applies authenticated live row");
    let relayed = relay.changes_since(Lsn(0));
    let relay_lineage = only_lineage(&lineages(
        &relay,
        &relayed,
        &tenant,
        &relay_author,
        relay.sync_incarnation(&tenant).expect("relay incarnation"),
    ));
    assert_eq!(
        relay_lineage, creator,
        "relay retains the original creator root and signature"
    );

    let schema_source = Database::open_memory();
    notes(&schema_source);
    let schema_id = Uuid::new_v4();
    insert(&schema_source, schema_id, "schema source");
    let (schema_changes, _, schema_ddl_source) = schema_source
        .checked_changes_since_with_arrivals(Lsn(0))
        .expect("schema source changes");
    let schema_incarnation = schema_source
        .sync_incarnation(&tenant)
        .expect("schema source incarnation");
    let schema_lineages = lineages(
        &schema_source,
        &schema_changes,
        &tenant,
        &author,
        schema_incarnation,
    );
    let schema_provenance = schema_source
        .outbound_ddl_provenance(&schema_changes, &schema_ddl_source)
        .expect("authenticated schema provenance");
    let received = crate::protocol::ReceivedDdlContext {
        tenant_id: tenant.clone(),
        source_node_id: author.node_id(),
        source_incarnation: schema_incarnation,
        entries: schema_provenance
            .iter()
            .map(|provenance| crate::protocol::ReceivedDdlEntry {
                source_ddl_lsn: provenance.source_ddl_lsn,
                ordinal: provenance.ordinal,
                table: provenance.table.clone(),
                table_generation: provenance.table_generation,
                digest: provenance.digest.clone(),
            })
            .collect(),
    };
    let schema_relay = Database::open_memory();
    schema_relay
        .apply_authenticated_received_changes_with_lineages(
            schema_changes.clone(),
            &HashMap::new(),
            SyncAdoption::Continuing,
            None,
            &tenant,
            &lineage_entries(&schema_changes, &schema_lineages),
            Some(&received),
            false,
        )
        .expect("relay applies authenticated schema-bearing row");
    let schema_relayed = schema_relay.changes_since(Lsn(0));
    assert_eq!(
        only_lineage(&lineages(
            &schema_relay,
            &schema_relayed,
            &tenant,
            &relay_author,
            schema_relay
                .sync_incarnation(&tenant)
                .expect("schema relay incarnation"),
        )),
        only_lineage(&schema_lineages),
        "schema-bearing relay retains the original creator root and signature"
    );
}

#[test]
fn received_memory_schema_then_row_relay_forwards_creator_generation() {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = TenantId::from("memory-received-schema-relay");
    let author = identity(&root, "author");
    let relay_author = identity(&root, "relay");
    let source = Database::open_memory();
    notes(&source);
    let id = Uuid::new_v4();
    insert(&source, id, "source row");
    let (changes, _, source_ddl_source) = source
        .checked_changes_since_with_arrivals(Lsn(0))
        .expect("source schema and row changes");
    let source_incarnation = source
        .sync_incarnation(&tenant)
        .expect("source incarnation");
    let source_lineages = lineages(&source, &changes, &tenant, &author, source_incarnation);
    let source_provenance = source
        .outbound_ddl_provenance(&changes, &source_ddl_source)
        .expect("source schema provenance");
    let received = crate::protocol::ReceivedDdlContext {
        tenant_id: tenant.clone(),
        source_node_id: author.node_id(),
        source_incarnation,
        entries: source_provenance
            .iter()
            .map(|provenance| crate::protocol::ReceivedDdlEntry {
                source_ddl_lsn: provenance.source_ddl_lsn,
                ordinal: provenance.ordinal,
                table: provenance.table.clone(),
                table_generation: provenance.table_generation,
                digest: provenance.digest.clone(),
            })
            .collect(),
    };
    let schema_only = ChangeSet {
        ddl: changes.ddl.clone(),
        ddl_lsn: changes.ddl_lsn.clone(),
        ..ChangeSet::default()
    };
    let rows_only = ChangeSet {
        rows: changes.rows.clone(),
        ..ChangeSet::default()
    };

    let relay = Database::open_memory();
    relay
        .apply_authenticated_received_changes_with_lineages(
            schema_only,
            &HashMap::new(),
            SyncAdoption::Continuing,
            None,
            &tenant,
            &[],
            Some(&received),
            true,
        )
        .expect("schema-less relay accepts authenticated schema");
    relay
        .apply_authenticated_received_changes_with_lineages(
            rows_only.clone(),
            &HashMap::new(),
            SyncAdoption::Continuing,
            None,
            &tenant,
            &lineage_entries(&rows_only, &source_lineages),
            None,
            false,
        )
        .expect("schema-only acceptance supplies the row generation");

    let (forwarded, _, relay_ddl_source) = relay
        .checked_changes_since_with_arrivals(Lsn(0))
        .expect("accepted schema and row are available to forward");
    let forwarded_incarnation = relay.sync_incarnation(&tenant).expect("relay incarnation");
    let forwarded_lineage = only_lineage(&lineages(
        &relay,
        &forwarded,
        &tenant,
        &relay_author,
        forwarded_incarnation,
    ));
    let forwarded_provenance = relay
        .outbound_ddl_provenance(&forwarded, &relay_ddl_source)
        .expect("accepted table generation has outbound provenance");
    assert_eq!(
        forwarded_lineage,
        only_lineage(&source_lineages),
        "the forwarded row retains the original creator evidence"
    );
    assert_eq!(
        forwarded_lineage.table_generation, 1,
        "the forwarded row retains the accepted first table generation"
    );
    assert_eq!(
        forwarded_provenance, source_provenance,
        "the schema-less relay forwards the accepted authoritative schema provenance"
    );
    assert_eq!(
        forwarded_provenance
            .iter()
            .filter(|provenance| provenance.table.as_deref() == Some("notes"))
            .map(|provenance| provenance.table_generation)
            .collect::<Vec<_>>(),
        vec![Some(1)],
        "the forwarded notes schema retains its accepted first generation"
    );
}

#[test]
fn memory_deletes_keep_creation_lineage_and_pending_obligation() {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = TenantId::from("memory-delete-lineage");
    let author = identity(&root, "author");
    let before_send = Database::open_memory();
    notes(&before_send);
    let incarnation = before_send
        .sync_incarnation(&tenant)
        .expect("memory incarnation");
    let base = before_send.current_lsn();
    let id = Uuid::new_v4();
    insert(&before_send, id, "created then deleted");
    let insert_lsn = before_send.current_lsn();
    delete(&before_send, id);
    let deleted_before_send = before_send.changes_since(base);
    let delete_lineages = lineages(
        &before_send,
        &deleted_before_send,
        &tenant,
        &author,
        incarnation,
    );
    let before_send_lineage = deleted_lineage(&deleted_before_send, &delete_lineages);
    assert!(
        before_send
            .has_durable_pending_delete_obligations()
            .expect("inspect pending delete"),
        "a local delete remains owed after an unsuccessful send"
    );
    assert_eq!(before_send_lineage.author_node_id, author.node_id());
    assert_eq!(before_send_lineage.author_database_incarnation, incarnation);
    assert_eq!(
        before_send_lineage.author_local_mutation_position,
        insert_lsn
    );
    assert_eq!(
        before_send_lineage.lineage_root,
        format!(
            "author:{}:{}:{}",
            author.node_id(),
            incarnation.to_hex(),
            insert_lsn.0
        )
    );
    assert!(
        !before_send_lineage.attestation.is_empty(),
        "a pre-push delete carries the creator signature"
    );
    let delete_only = ChangeSet {
        rows: deleted_before_send
            .rows
            .iter()
            .filter(|row| row.deleted)
            .cloned()
            .collect(),
        ..ChangeSet::default()
    };
    before_send
        .validate_received_row_lineages(
            &tenant,
            &delete_only,
            &lineage_entries(&delete_only, &delete_lineages),
        )
        .expect("pre-push delete creator tuple and signature validate");

    let accepted_db = Database::open_memory();
    notes(&accepted_db);
    let accepted_incarnation = accepted_db
        .sync_incarnation(&tenant)
        .expect("accepted-delete incarnation");
    let pushed_id = Uuid::new_v4();
    let live_base = accepted_db.current_lsn();
    insert(&accepted_db, pushed_id, "created before first push");
    let pushed = accepted_db.changes_since(live_base);
    let pushed_lineage = only_lineage(&lineages(
        &accepted_db,
        &pushed,
        &tenant,
        &author,
        accepted_incarnation,
    ));
    let delete_base = accepted_db.current_lsn();
    delete(&accepted_db, pushed_id);
    assert!(
        note_is_absent(&accepted_db, pushed_id),
        "the local delete is absent before the hub acknowledgement"
    );
    let accepted = accepted_db
        .changes_since(delete_base)
        .rows
        .into_iter()
        .filter(|row| row.deleted)
        .collect::<Vec<_>>();
    let accepted_changes = ChangeSet {
        rows: accepted.clone(),
        ..ChangeSet::default()
    };
    assert_eq!(
        deleted_lineage(
            &accepted_changes,
            &lineages(
                &accepted_db,
                &accepted_changes,
                &tenant,
                &author,
                accepted_incarnation,
            ),
        ),
        pushed_lineage,
        "a post-push delete retains its original creation lineage"
    );
    assert!(
        accepted_db
            .has_durable_pending_delete_obligations()
            .expect("inspect accepted-delete pending state"),
        "the post-push delete is pending before the hub acknowledgement"
    );
    accepted_db
        .record_hub_push_reply_effects_while_authoritative(
            &tenant,
            "hub",
            &[],
            &[],
            &accepted,
            Lsn(17),
        )
        .expect("record accepted delete");
    assert!(
        note_is_absent(&accepted_db, pushed_id),
        "the accepted local delete remains absent after the hub acknowledgement"
    );
    assert!(
        !accepted_db
            .has_durable_pending_delete_obligations()
            .expect("inspect accepted delete"),
        "hub acknowledgement retires the pending send obligation"
    );
    let reupload = accepted_db
        .append_destination_reupload_deletes(ChangeSet::default(), Lsn(u64::MAX))
        .expect("build accepted destination reupload");
    assert_eq!(
        reupload.rows, accepted,
        "accepted deletion is carried to a new destination"
    );

    let refused_db = Database::open_memory();
    notes(&refused_db);
    let refused_incarnation = refused_db
        .sync_incarnation(&tenant)
        .expect("refused-delete incarnation");
    let after_acceptance = refused_db.current_lsn();
    let refused_id = Uuid::new_v4();
    insert(&refused_db, refused_id, "refused delete");
    delete(&refused_db, refused_id);
    let refused = refused_db
        .changes_since(after_acceptance)
        .rows
        .into_iter()
        .filter(|row| row.deleted)
        .collect::<Vec<_>>();
    let _ = lineages(
        &refused_db,
        &ChangeSet {
            rows: refused.clone(),
            ..ChangeSet::default()
        },
        &tenant,
        &author,
        refused_incarnation,
    );
    assert!(
        refused_db
            .has_durable_pending_delete_obligations()
            .expect("inspect refused delete"),
        "a delete remains pending until the hub gives its outcome"
    );
    refused_db
        .record_hub_push_reply_effects_while_authoritative(
            &tenant,
            "hub",
            &refused,
            &[],
            &[],
            Lsn(18),
        )
        .expect("record refused delete");
    assert!(
        !refused_db
            .has_durable_pending_delete_obligations()
            .expect("inspect refused delete retirement"),
        "terminal refusal retires the pending delete obligation"
    );
    let after_refusal = refused_db
        .append_destination_reupload_deletes(ChangeSet::default(), Lsn(u64::MAX))
        .expect("inspect refused delete retirement");
    assert!(
        !after_refusal
            .rows
            .iter()
            .any(|row| row.natural_key == refused[0].natural_key),
        "terminal refusal retires a delete instead of reuploading it"
    );
}

#[test]
fn authenticated_fresh_creator_delete_is_the_only_keep_first_delete_continuation() {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = TenantId::from("authenticated-keep-first-delete");
    let creator = identity(&root, "creator");
    let relay = identity(&root, "relay");
    let source = Database::open_memory();
    keep_first_notes(&source);
    let source_incarnation = source
        .sync_incarnation(&tenant)
        .expect("creator database incarnation");
    let id = Uuid::new_v4();
    let before_insert = source.current_lsn();
    insert(&source, id, "creator-owned row");
    let inserted = source.changes_since(before_insert);
    let insert_lsn = inserted.rows.first().expect("creator emits one insert").lsn;
    let inserted_lineages = lineages(&source, &inserted, &tenant, &creator, source_incarnation);
    let before_delete = source.current_lsn();
    delete(&source, id);
    let deleted = source.changes_since(before_delete);
    let delete_lsn = deleted
        .rows
        .iter()
        .find(|row| row.deleted)
        .expect("creator emits one delete")
        .lsn;
    let deleted_lineages = lineages(&source, &deleted, &tenant, &creator, source_incarnation);
    let inserted_entries = lineage_entries(&inserted, &inserted_lineages);
    let deleted_entries = lineage_entries(&deleted, &deleted_lineages);

    let receiver = Database::open_memory();
    keep_first_notes(&receiver);
    receiver
        .apply_authenticated_received_changes_with_receipt_and_lineages(
            inserted.clone(),
            &HashMap::new(),
            SyncAdoption::Continuing,
            SyncApplyReceipt {
                tenant_id: tenant.clone(),
                node_id: creator.node_id(),
                incarnation: source_incarnation,
                source_lsn: insert_lsn,
                dependency_complete: false,
            },
            Some("receiver-hub"),
            &inserted_entries,
            None,
        )
        .expect("receiver commits the creator row and its authenticated sidecar");
    let fresh = receiver
        .apply_authenticated_received_changes_with_receipt_and_lineages(
            deleted.clone(),
            &HashMap::from([(delete_lsn, None)]),
            SyncAdoption::Continuing,
            SyncApplyReceipt {
                tenant_id: tenant.clone(),
                node_id: creator.node_id(),
                incarnation: source_incarnation,
                source_lsn: delete_lsn,
                dependency_complete: false,
            },
            Some("receiver-hub"),
            &deleted_entries,
            None,
        )
        .expect("an unordered delete from the exact authenticated creator applies");
    assert_eq!(fresh.applied_rows, 1, "creator delete applies exactly once");
    assert_eq!(fresh.skipped_rows, 0, "creator delete is not suppressed");
    assert!(fresh.conflicts.is_empty(), "creator delete has no conflict");
    assert!(
        note_is_absent(&receiver, id),
        "creator delete removes the row"
    );

    let relayed_receiver = Database::open_memory();
    keep_first_notes(&relayed_receiver);
    relayed_receiver
        .apply_authenticated_received_changes_with_receipt_and_lineages(
            inserted.clone(),
            &HashMap::new(),
            SyncAdoption::Continuing,
            SyncApplyReceipt {
                tenant_id: tenant.clone(),
                node_id: creator.node_id(),
                incarnation: source_incarnation,
                source_lsn: insert_lsn,
                dependency_complete: false,
            },
            Some("receiver-hub"),
            &inserted_entries,
            None,
        )
        .expect("relay case starts with the same committed creator row");
    let relayed = relayed_receiver
        .apply_authenticated_received_changes_with_receipt_and_lineages(
            deleted.clone(),
            &HashMap::from([(delete_lsn, None)]),
            SyncAdoption::Continuing,
            SyncApplyReceipt {
                tenant_id: tenant.clone(),
                node_id: relay.node_id(),
                incarnation: Incarnation(777),
                source_lsn: delete_lsn,
                dependency_complete: false,
            },
            Some("receiver-hub"),
            &deleted_entries,
            None,
        )
        .expect("a relayed delete is a handled terminal refusal");
    assert_eq!(
        relayed.applied_rows, 0,
        "relay may not delete KEEP FIRST data"
    );
    assert_eq!(
        relayed.skipped_rows, 1,
        "relay delete is explicitly refused"
    );
    assert!(
        !note_is_absent(&relayed_receiver, id),
        "the same signed root relayed by another authenticated node stays present"
    );

    let carried_receiver = Database::open_memory();
    keep_first_notes(&carried_receiver);
    carried_receiver
        .apply_authenticated_received_changes_with_receipt_and_lineages(
            inserted.clone(),
            &HashMap::new(),
            SyncAdoption::Continuing,
            SyncApplyReceipt {
                tenant_id: tenant.clone(),
                node_id: creator.node_id(),
                incarnation: source_incarnation,
                source_lsn: insert_lsn,
                dependency_complete: false,
            },
            Some("receiver-hub"),
            &inserted_entries,
            None,
        )
        .expect("carried-arrival case starts with the same committed creator row");
    let carried = carried_receiver
        .apply_authenticated_received_changes_with_receipt_and_lineages(
            deleted,
            &HashMap::from([(delete_lsn, Some(Lsn(501)))]),
            SyncAdoption::Continuing,
            SyncApplyReceipt {
                tenant_id: tenant,
                node_id: creator.node_id(),
                incarnation: source_incarnation,
                source_lsn: delete_lsn,
                dependency_complete: false,
            },
            Some("receiver-hub"),
            &deleted_entries,
            None,
        )
        .expect("a carried creator delete is a handled stale/no-op path");
    assert_eq!(carried.applied_rows, 0, "carried delete does not apply");
    assert_eq!(
        carried.skipped_rows, 1,
        "carried delete is explicitly suppressed"
    );
    assert!(
        !note_is_absent(&carried_receiver, id),
        "a creator delete with an established arrival cannot bypass KEEP FIRST"
    );
}

#[test]
fn authenticated_fresh_creator_delete_does_not_override_server_wins() {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = TenantId::from("authenticated-server-wins-delete");
    let creator = identity(&root, "creator");
    let source = Database::open_memory();
    server_wins_work_claims(&source);
    let source_incarnation = source
        .sync_incarnation(&tenant)
        .expect("creator database incarnation");
    let claim_key = "job:1";
    let before_insert = source.current_lsn();
    insert_work_claim(&source, claim_key);
    let inserted = source.changes_since(before_insert);
    let insert_lsn = inserted
        .rows
        .first()
        .expect("creator emits one work claim")
        .lsn;
    let inserted_lineages = lineages(&source, &inserted, &tenant, &creator, source_incarnation);
    let before_delete = source.current_lsn();
    source
        .execute(
            "DELETE FROM work_claims WHERE claim_key = $claim_key",
            &HashMap::from([("claim_key".to_string(), Value::Text(claim_key.to_string()))]),
        )
        .expect("creator deletes its work claim");
    let deleted = source.changes_since(before_delete);
    let delete_lsn = deleted
        .rows
        .iter()
        .find(|row| row.deleted)
        .expect("creator emits one delete")
        .lsn;
    let deleted_lineages = lineages(&source, &deleted, &tenant, &creator, source_incarnation);
    let inserted_entries = lineage_entries(&inserted, &inserted_lineages);
    let deleted_entries = lineage_entries(&deleted, &deleted_lineages);

    let receiver = Database::open_memory();
    server_wins_work_claims(&receiver);
    receiver
        .apply_authenticated_received_changes_with_receipt_and_lineages(
            inserted.clone(),
            &HashMap::new(),
            SyncAdoption::Continuing,
            SyncApplyReceipt {
                tenant_id: tenant.clone(),
                node_id: creator.node_id(),
                incarnation: source_incarnation,
                source_lsn: insert_lsn,
                dependency_complete: false,
            },
            Some("receiver-hub"),
            &inserted_entries,
            None,
        )
        .expect("server-wins receiver commits the creator row and its sidecar");
    let refused = receiver
        .apply_authenticated_received_changes_with_receipt_and_lineages(
            deleted,
            &HashMap::from([(delete_lsn, None)]),
            SyncAdoption::Continuing,
            SyncApplyReceipt {
                tenant_id: tenant,
                node_id: creator.node_id(),
                incarnation: source_incarnation,
                source_lsn: delete_lsn,
                dependency_complete: false,
            },
            Some("receiver-hub"),
            &deleted_entries,
            None,
        )
        .expect("server-wins refusal still acknowledges the handled source unit");
    assert_eq!(refused.applied_rows, 0, "server-wins keeps its row");
    assert_eq!(refused.skipped_rows, 1, "server-wins refuses the delete");
    assert_eq!(refused.conflicts.len(), 1, "server-wins names the refusal");
    assert_eq!(
        refused.conflicts[0].resolution,
        ConflictPolicy::ServerWins,
        "an exact fresh creator receipt cannot bypass SERVER WINS"
    );
    assert!(
        !work_claim_is_absent(&receiver, claim_key),
        "the server-wins row remains after the creator delete"
    );
}

#[test]
fn received_alter_and_fresh_creator_delete_share_the_same_keep_first_decision() {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = TenantId::from("received-alter-fresh-creator-delete");
    let creator = identity(&root, "creator");
    let source = Database::open_memory();
    keep_first_notes(&source);
    let source_incarnation = source
        .sync_incarnation(&tenant)
        .expect("creator database incarnation");
    let id = Uuid::new_v4();
    let before_insert = source.current_lsn();
    insert(&source, id, "creator-owned row");
    let inserted = source.changes_since(before_insert);
    let inserted_lineages = lineages(&source, &inserted, &tenant, &creator, source_incarnation);
    let inserted_entries = lineage_entries(&inserted, &inserted_lineages);

    let receiver = Database::open_memory();
    keep_first_notes(&receiver);
    receiver
        .apply_authenticated_received_changes_with_lineages(
            inserted,
            &HashMap::new(),
            SyncAdoption::Continuing,
            None,
            &tenant,
            &inserted_entries,
            None,
            false,
        )
        .expect("receiver commits the creator row and its authenticated sidecar");

    let before_mixed = source.current_lsn();
    source
        .execute("ALTER TABLE notes ADD COLUMN tag TEXT", &HashMap::new())
        .expect("creator alters the same table before deleting its row");
    delete(&source, id);
    let (mixed, arrivals, ddl_source) = source
        .checked_changes_since_with_arrivals(before_mixed)
        .expect("collect received ALTER and delete");
    let delete_lsn = mixed
        .rows
        .iter()
        .find(|row| row.deleted)
        .expect("mixed unit contains the creator delete")
        .lsn;
    let mixed_lineages = lineages(&source, &mixed, &tenant, &creator, source_incarnation);
    let mixed_entries = lineage_entries(&mixed, &mixed_lineages);
    let received = crate::protocol::ReceivedDdlContext {
        tenant_id: tenant.clone(),
        source_node_id: creator.node_id(),
        source_incarnation,
        entries: source
            .outbound_ddl_provenance(&mixed, &ddl_source)
            .expect("authenticated ALTER provenance")
            .into_iter()
            .map(|provenance| crate::protocol::ReceivedDdlEntry {
                source_ddl_lsn: provenance.source_ddl_lsn,
                ordinal: provenance.ordinal,
                table: provenance.table,
                table_generation: provenance.table_generation,
                digest: provenance.digest,
            })
            .collect(),
    };
    let applied = receiver
        .apply_authenticated_received_changes_with_receipt_and_lineages(
            mixed,
            &arrivals,
            SyncAdoption::Continuing,
            SyncApplyReceipt {
                tenant_id: tenant,
                node_id: creator.node_id(),
                incarnation: source_incarnation,
                source_lsn: delete_lsn,
                dependency_complete: true,
            },
            None,
            &mixed_entries,
            Some(&received),
        )
        .expect("received-DDL preflight and detached apply agree on the creator delete");
    assert_eq!(applied.applied_rows, 1, "creator delete applies once");
    assert_eq!(applied.skipped_rows, 0, "creator delete is not suppressed");
    assert!(
        applied.conflicts.is_empty(),
        "creator delete has no conflict"
    );
    assert!(
        note_is_absent(&receiver, id),
        "the row is deleted after the mixed received-schema commit"
    );
    receiver
        .execute(
            "INSERT INTO notes (id, body, tag) VALUES ($id, $body, $tag)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                (
                    "body".to_string(),
                    Value::Text("schema applied".to_string()),
                ),
                ("tag".to_string(), Value::Text("present".to_string())),
            ]),
        )
        .expect("the received ALTER commits with the accepted delete");
}

#[test]
fn fresh_creator_delete_race_errors_without_advancing_its_receipt() {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = TenantId::from("fresh-creator-delete-commit-race");
    let creator = identity(&root, "creator");
    let source = Database::open_memory();
    keep_first_notes(&source);
    let source_incarnation = source
        .sync_incarnation(&tenant)
        .expect("creator database incarnation");
    let id = Uuid::new_v4();
    let before_insert = source.current_lsn();
    insert(&source, id, "creator-owned row");
    let inserted = source.changes_since(before_insert);
    let inserted_lineages = lineages(&source, &inserted, &tenant, &creator, source_incarnation);
    let before_delete = source.current_lsn();
    delete(&source, id);
    let deleted = source.changes_since(before_delete);
    let delete_lsn = deleted
        .rows
        .iter()
        .find(|row| row.deleted)
        .expect("creator emits one delete")
        .lsn;
    let deleted_entries = lineage_entries(
        &deleted,
        &lineages(&source, &deleted, &tenant, &creator, source_incarnation),
    );

    let receiver = Arc::new(Database::open_memory());
    keep_first_notes(&receiver);
    receiver
        .apply_authenticated_received_changes_with_lineages(
            inserted.clone(),
            &HashMap::new(),
            SyncAdoption::Continuing,
            None,
            &tenant,
            &lineage_entries(&inserted, &inserted_lineages),
            None,
            false,
        )
        .expect("receiver commits the creator row and its authenticated sidecar");
    let pause = receiver.pause_before_sync_apply_commit_for_test();
    std::thread::scope(|scope| {
        let applying = Arc::clone(&receiver);
        let applying_tenant = tenant.clone();
        let applying_creator = Arc::clone(&creator);
        let applying_deleted = deleted.clone();
        let applying_entries = deleted_entries.clone();
        let apply = scope.spawn(move || {
            applying.mark_this_thread_for_sync_apply_pre_commit_pause_for_test();
            applying.apply_authenticated_received_changes_with_receipt_and_lineages(
                applying_deleted,
                &HashMap::from([(delete_lsn, None)]),
                SyncAdoption::Continuing,
                SyncApplyReceipt {
                    tenant_id: applying_tenant,
                    node_id: applying_creator.node_id(),
                    incarnation: source_incarnation,
                    source_lsn: delete_lsn,
                    dependency_complete: false,
                },
                None,
                &applying_entries,
                None,
            )
        });
        assert!(
            pause.wait_until_reached(std::time::Duration::from_secs(5)),
            "the delete must pause after permission classification and before commit"
        );
        update(&receiver, id, "local replacement wins the commit race");
        // Release before observing the worker result so an assertion cannot
        // strand the scoped thread at the deterministic test fence.
        pause.release();
        assert!(
            apply
                .join()
                .expect("paused sync apply thread does not panic")
                .is_err(),
            "a changed row after classification makes the whole sync apply fail"
        );
    });
    assert!(
        !note_is_absent(&receiver, id),
        "the locally replaced row remains after the failed sync apply"
    );
    assert_eq!(
        receiver
            .persisted_sync_applied_push_watermark_for_node_incarnation(
                &tenant,
                &creator.node_id(),
                source_incarnation,
            )
            .expect("inspect receipt watermark"),
        None,
        "a failed whole sync apply never advances its receipt"
    );
}

#[test]
fn accepted_local_delete_blocks_only_older_same_source_replay_and_stays_pushable() {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = TenantId::from("accepted-local-delete-replay");
    let edge_identity = identity(&root, "edge");
    let edge = Database::open_memory();
    notes(&edge);
    let edge_incarnation = edge.sync_incarnation(&tenant).expect("edge incarnation");
    let id = Uuid::new_v4();
    let before_insert = edge.current_lsn();
    insert(&edge, id, "before delete");
    let stale_changes = edge.changes_since(before_insert);
    let stale_live = stale_changes
        .rows
        .iter()
        .find(|row| !row.deleted)
        .cloned()
        .expect("capture the hub's stale live row");
    let stale_lsn = stale_live.lsn;
    let stale_lineages = lineages(
        &edge,
        &stale_changes,
        &tenant,
        &edge_identity,
        edge_incarnation,
    );
    let stale_lineage = only_lineage(&stale_lineages);
    let stale_lineage_entries = lineage_entries(&stale_changes, &stale_lineages);

    edge.execute(
        "DELETE FROM notes WHERE id = $id",
        &HashMap::from([("id".to_string(), Value::Uuid(id))]),
    )
    .expect("local delete");
    let accepted_delete = edge
        .changes_since(stale_lsn)
        .rows
        .into_iter()
        .find(|row| row.deleted)
        .expect("capture local delete");
    edge.record_hub_accepted_rows(
        std::slice::from_ref(&accepted_delete),
        Lsn(900),
        Some("hub"),
    )
    .expect("record accepted local delete");
    assert!(
        !edge.row_change_arrived_by_sync(&accepted_delete),
        "an AcceptedLocal delete must remain eligible to repair a hub that regressed"
    );

    edge.apply_authenticated_received_changes_with_lineages(
        stale_changes.clone(),
        &HashMap::from([(stale_lsn, Some(Lsn(800)))]),
        SyncAdoption::Continuing,
        None,
        &tenant,
        &stale_lineage_entries,
        None,
        false,
    )
    .expect("signed stale hub replay is a safe no-op");
    assert!(
        note_is_absent(&edge, id),
        "a stale signed hub replay must not resurrect an acknowledged local delete in this session"
    );

    edge.apply_authenticated_received_changes_with_lineages(
        stale_changes.clone(),
        &HashMap::from([(stale_lsn, Some(Lsn(900)))]),
        SyncAdoption::Continuing,
        None,
        &tenant,
        &stale_lineage_entries,
        None,
        false,
    )
    .expect("signed same-commit authoritative refusal repair applies");
    assert!(
        !note_is_absent(&edge, id),
        "an equal arrival is the hub's same-commit authoritative repair, not a stale replay"
    );

    let before_second_delete = edge.current_lsn();
    delete(&edge, id);
    let second_delete = edge
        .changes_since(before_second_delete)
        .rows
        .into_iter()
        .find(|row| row.deleted)
        .expect("second local delete change");
    let second_delete_changes = ChangeSet {
        rows: vec![second_delete.clone()],
        ..ChangeSet::default()
    };
    assert_eq!(
        only_lineage(&lineages(
            &edge,
            &second_delete_changes,
            &tenant,
            &edge_identity,
            edge_incarnation,
        )),
        stale_lineage,
        "the signed hub repair remains locally deletable and pushable with its original creator tuple"
    );
    edge.record_hub_accepted_rows(
        std::slice::from_ref(&second_delete),
        Lsn(1_000),
        Some("hub"),
    )
    .expect("record old-source accepted delete");
    edge.apply_authenticated_received_changes_with_lineages(
        stale_changes,
        &HashMap::from([(stale_lsn, Some(Lsn(1)))]),
        SyncAdoption::ReadoptingSource,
        None,
        &tenant,
        &stale_lineage_entries,
        None,
        false,
    )
    .expect("signed new-source authoritative history applies");
    assert!(
        !note_is_absent(&edge, id),
        "re-adopting a source must bypass an old source's AcceptedLocal tombstone"
    );
}

#[cfg(feature = "test-seams")]
#[tokio::test]
async fn failed_memory_delete_send_keeps_the_obligation_pending() {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = TenantId::from("memory-delete-send");
    let author = identity(&root, "author");
    let db = Arc::new(Database::open_memory());
    notes(&db);
    let id = Uuid::new_v4();
    insert(&db, id, "delete before unavailable hub");
    delete(&db, id);
    let broker = crate::transport::in_process::InProcessBroker::new();
    let hub = "unavailable-hub";
    let _ = broker.server_as(hub);
    let client = crate::sync_client::SyncClient::with_authenticated_transport_and_identity_for_test(
        db.clone(),
        broker.client_as(&author.node_id()),
        tenant.clone(),
        author,
    );
    assert!(
        client.push().await.is_err(),
        "unavailable hub refuses the send"
    );
    assert!(
        db.has_durable_pending_delete_obligations()
            .expect("inspect pending delete after failed send"),
        "a failed send leaves the local delete pending for the next push"
    );
}

#[test]
fn received_memory_deletes_keep_inherited_lineage() {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = TenantId::from("memory-relay-delete");
    let author = identity(&root, "author");
    let relay_author = identity(&root, "relay");
    let source = Database::open_memory();
    notes(&source);
    let base = source.current_lsn();
    let id = Uuid::new_v4();
    insert(&source, id, "source");
    let source_incarnation = source
        .sync_incarnation(&tenant)
        .expect("source incarnation");
    let inserted = source.changes_since(base);
    let inserted_lineages = lineages(&source, &inserted, &tenant, &author, source_incarnation);
    delete(&source, id);
    let deleted = source.changes_since(inserted.rows[0].lsn);
    let deleted_lineages = lineages(&source, &deleted, &tenant, &author, source_incarnation);

    let relay = Database::open_memory();
    notes(&relay);
    relay
        .apply_authenticated_received_changes_with_lineages(
            inserted.clone(),
            &HashMap::new(),
            SyncAdoption::Continuing,
            None,
            &tenant,
            &lineage_entries(&inserted, &inserted_lineages),
            None,
            false,
        )
        .expect("relay applies live row");
    relay
        .apply_authenticated_received_changes_with_lineages(
            deleted.clone(),
            &HashMap::new(),
            SyncAdoption::Continuing,
            None,
            &tenant,
            &lineage_entries(&deleted, &deleted_lineages),
            None,
            false,
        )
        .expect("relay applies delete");
    let relayed_delete = relay.changes_since(Lsn(0));
    let relay_incarnation = relay.sync_incarnation(&tenant).expect("relay incarnation");
    let ordinary_delete = relayed_delete
        .rows
        .iter()
        .find(|row| row.deleted)
        .expect("relay has received delete");
    let ordinary_outbound =
        crate::sync_client::drop_rows_that_arrived_by_sync(&relay, relayed_delete.clone());
    assert!(
        !ordinary_outbound.rows.contains(ordinary_delete),
        "ordinary relay selection suppresses accepted received ancestry"
    );
    relay
        .change_retention_sync_peer(&tenant, "destination-hub")
        .expect("select explicit destination reupload");
    let held_through = relay
        .destination_reupload_epoch(&tenant, "destination-hub")
        .expect("read destination reupload")
        .expect("explicit destination reupload is present")
        .0;
    let reuploaded_delete = relay
        .append_destination_reupload_deletes(ChangeSet::default(), held_through)
        .expect("build destination delete reupload");
    assert_eq!(
        deleted_lineage(
            &reuploaded_delete,
            &lineages(
                &relay,
                &reuploaded_delete,
                &tenant,
                &relay_author,
                relay_incarnation,
            )
        ),
        deleted_lineage(&deleted, &deleted_lineages),
        "a received delete retains its inherited creator root and signature"
    );
}

#[test]
fn new_memory_database_is_a_new_incarnation_without_old_state() {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = TenantId::from("memory-incarnation");
    let identity_path = root.path().join("author.key");
    let author = identity(&root, "author");
    let first = Arc::new(Database::open_memory());
    notes(&first);
    let first_incarnation = first.sync_incarnation(&tenant).expect("first incarnation");
    let first_base = first.current_lsn();
    let live_id = Uuid::new_v4();
    insert(&first, live_id, "first live row");
    let first_live_changes = first.changes_since(first_base);
    let first_live_lineage = only_lineage(&lineages(
        &first,
        &first_live_changes,
        &tenant,
        &author,
        first_incarnation,
    ));
    let deleted_id = Uuid::new_v4();
    let delete_create_base = first.current_lsn();
    insert(&first, deleted_id, "first pending delete");
    let delete_creation = first.changes_since(delete_create_base);
    let _ = lineages(
        &first,
        &delete_creation,
        &tenant,
        &author,
        first_incarnation,
    );
    let delete_base = first.current_lsn();
    delete(&first, deleted_id);
    let first_delete = first.changes_since(delete_base);
    let first_delete_lineage = deleted_lineage(
        &first_delete,
        &lineages(&first, &first_delete, &tenant, &author, first_incarnation),
    );
    assert!(
        first
            .has_durable_pending_delete_obligations()
            .expect("first pending delete"),
        "the first memory life carries a pending delete"
    );
    drop(first);
    let reloaded_author = Arc::new(
        FabricIdentity::load_or_generate(&identity_path).expect("reload explicit transport key"),
    );
    assert_eq!(
        reloaded_author.node_id(),
        author.node_id(),
        "the explicit transport key survives independently of database state"
    );

    let second = Arc::new(Database::open_memory());
    assert_ne!(
        second
            .sync_incarnation(&tenant)
            .expect("second incarnation"),
        first_incarnation,
        "a new in-memory database is a distinct life even with the same transport key"
    );
    assert!(
        second.changes_since(Lsn(0)).rows.is_empty(),
        "new life has no old rows"
    );
    assert_eq!(
        second
            .persisted_sync_watermarks(&tenant)
            .expect("memory cursors"),
        (Lsn(0), Lsn(0)),
        "new life has no old cursors"
    );
    assert!(
        !second
            .has_durable_pending_delete_obligations()
            .expect("memory delete state"),
        "new life has no old pending deletes or lineage"
    );
    notes(&second);
    assert!(
        second
            .scan("notes", second.snapshot())
            .expect("scan fresh memory database")
            .is_empty(),
        "the fresh memory database has neither first-life live row nor deleted row"
    );
    let second_base = second.current_lsn();
    insert(&second, deleted_id, "second life");
    let second_lineage = only_lineage(&lineages(
        &second,
        &second.changes_since(second_base),
        &tenant,
        &reloaded_author,
        second
            .sync_incarnation(&tenant)
            .expect("second incarnation"),
    ));
    assert_ne!(
        second_lineage, first_delete_lineage,
        "the same transport key does not carry old database lineage into a new memory life"
    );
    assert_ne!(
        second_lineage, first_live_lineage,
        "the fresh memory database cannot inherit a different first-life row lineage"
    );
}
