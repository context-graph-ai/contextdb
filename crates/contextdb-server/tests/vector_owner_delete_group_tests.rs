//! A same-commit delete can carry several vector owners. Each remote owner
//! group must delete its matching local ANN entry without consuming the next
//! owner's mapping.

use contextdb_core::{Lsn, Value, VectorIndexRef};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy, SyncAdoption};
use contextdb_server::{InProcessBroker, SyncClient, SyncServer};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

const DDL: &str = "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT, embedding VECTOR(3)) SYNC CONFLICT KEEP LATEST";

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn assert_exact_mixed_vector_outbound_provenance(db: &Database, since: Lsn) {
    let changes = db.changes_since(since);
    let a_row = changes
        .rows
        .iter()
        .find(|row| !row.deleted && row.natural_key.value == Value::Int64(1))
        .expect("pulled A row");
    let b_row = changes
        .rows
        .iter()
        .find(|row| !row.deleted && row.natural_key.value == Value::Int64(2))
        .expect("re-emitted B row");
    assert_eq!(
        a_row.lsn, b_row.lsn,
        "the pulled and policy-refused owners share one local sync commit"
    );

    let a_owner = db
        .query_vector(
            VectorIndexRef::new("notes", "embedding"),
            &[1.0, 0.0, 0.0],
            1,
            None,
            db.snapshot(),
        )
        .expect("find A vector owner")[0]
        .0;
    let b_owner = db
        .query_vector(
            VectorIndexRef::new("notes", "embedding"),
            &[0.0, 1.0, 0.0],
            1,
            None,
            db.snapshot(),
        )
        .expect("find B vector owner")[0]
        .0;
    let a_vector = changes
        .vectors
        .iter()
        .find(|vector| !vector.vector.is_empty() && vector.row_id == a_owner)
        .expect("pulled A vector");
    let b_vector = changes
        .vectors
        .iter()
        .find(|vector| !vector.vector.is_empty() && vector.row_id == b_owner)
        .unwrap_or_else(|| {
            panic!(
                "re-emitted B vector for owner {b_owner:?}; changes were {:?}",
                changes.vectors
            )
        });
    assert_eq!(a_vector.lsn, a_row.lsn);
    assert_eq!(b_vector.lsn, b_row.lsn);

    assert!(
        db.row_change_arrived_by_sync(a_row),
        "the Pulled A row is absent from outbound work"
    );
    assert!(
        db.vector_change_arrived_by_sync(a_vector),
        "the Pulled A vector is absent from outbound work"
    );
    assert!(
        !db.row_change_arrived_by_sync(b_row),
        "the AcceptedLocal B repair row remains outbound"
    );
    assert!(
        !db.vector_change_arrived_by_sync(b_vector),
        "the AcceptedLocal B repair vector remains outbound"
    );
}

/// One sync commit can contain a newly pulled vector owner and a different
/// owner whose incoming value KEEP FIRST refuses. The retained owner is
/// re-emitted as AcceptedLocal in that same table and commit. Provenance must
/// therefore be exact per owner, and the distinction must survive reopen.
#[test]
fn mixed_same_commit_vector_provenance_is_exact_and_durable() {
    const KEEP_FIRST_DDL: &str = "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT, embedding VECTOR(3)) SYNC CONFLICT KEEP FIRST";
    let temp = tempfile::TempDir::new().expect("tempdir");
    let edge_path = temp.path().join("mixed-vector-provenance.db");
    let source = Database::open_memory();
    let edge = Database::open(&edge_path).expect("open durable edge");
    for db in [&source, &edge] {
        db.execute(KEEP_FIRST_DDL, &empty())
            .expect("declare keep-first vector table");
    }
    edge.execute(
        "INSERT INTO notes VALUES (2, 'edge-authoritative', '[0,1,0]')",
        &empty(),
    )
    .expect("seed edge-authoritative B");

    let source_before = source.current_lsn();
    source
        .execute(
            "INSERT INTO notes VALUES \
             (1, 'pulled-a', '[1,0,0]'), \
             (2, 'incoming-b-refused', '[0,0,1]')",
            &empty(),
        )
        .expect("author one mixed source commit");
    let incoming = source.changes_since(source_before);
    let source_lsn = incoming.rows.first().expect("mixed source rows").lsn;
    assert!(
        incoming.rows.iter().all(|row| row.lsn == source_lsn)
            && incoming
                .vectors
                .iter()
                .all(|vector| vector.lsn == source_lsn),
        "fixture rows and vectors share one source commit"
    );

    let edge_before = edge.current_lsn();
    let result = edge
        .apply_synced_changes(
            incoming,
            &edge.conflict_policies(),
            &HashMap::from([(source_lsn, Some(Lsn(500)))]),
            SyncAdoption::Continuing,
        )
        .expect("apply mixed source commit");
    assert_eq!(result.applied_rows, 1, "A is newly pulled");
    assert_eq!(result.skipped_rows, 1, "B is refused and re-emitted");
    assert_exact_mixed_vector_outbound_provenance(&edge, edge_before);

    edge.close().expect("close durable edge");
    drop(edge);
    let reopened = Database::open(&edge_path).expect("reopen durable edge");
    assert_exact_mixed_vector_outbound_provenance(&reopened, edge_before);
}

#[test]
fn same_lsn_multi_owner_vector_deletes_remove_each_ann_owner() {
    let sender = Database::open_memory();
    let receiver = Database::open_memory();
    for db in [&sender, &receiver] {
        db.execute(DDL, &empty()).expect("declare vector table");
    }
    sender
        .execute(
            "INSERT INTO notes VALUES (1, 'first', '[1,0,0]'), (2, 'second', '[0,1,0]')",
            &empty(),
        )
        .expect("seed two vector owners in one commit");
    let policies = sender.conflict_policies();
    receiver
        .apply_synced_changes(
            sender.changes_since(Lsn(0)),
            &policies,
            &HashMap::new(),
            SyncAdoption::Continuing,
        )
        .expect("apply seed");

    let seed_lsn = sender.current_lsn();
    sender
        .execute("DELETE FROM notes", &empty())
        .expect("delete both owners in one commit");
    let deletes = sender.changes_since(seed_lsn);
    assert_eq!(deletes.rows.iter().filter(|row| row.deleted).count(), 2);
    assert_eq!(
        deletes
            .vectors
            .iter()
            .filter(|vector| vector.vector.is_empty())
            .count(),
        2,
        "both owner tombstones travel in the delete group"
    );
    receiver
        .apply_synced_changes(
            deletes,
            &policies,
            &HashMap::new(),
            SyncAdoption::Continuing,
        )
        .expect("apply both owner deletes");

    assert!(
        receiver
            .execute("SELECT * FROM notes", &empty())
            .expect("scan receiver")
            .rows
            .is_empty(),
        "both relational owners are absent"
    );
    for query in [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]] {
        assert!(
            receiver
                .query_vector(
                    VectorIndexRef::new("notes", "embedding"),
                    &query,
                    10,
                    None,
                    receiver.snapshot(),
                )
                .expect("ANN query")
                .is_empty(),
            "each deleted owner is absent from ANN"
        );
    }
}

/// A retained history can contain the owner's earlier relational insert and
/// later relational/vector tombstones while omitting the superseded vector
/// insert. A fresh receiver must pair the tombstone with the delete row, not
/// consume it while visiting the older live row.
#[test]
fn fresh_receiver_applies_retained_vector_tombstone_history() {
    let sender = Database::open_memory();
    let temp = tempfile::TempDir::new().expect("tempdir");
    let receiver =
        Database::open(temp.path().join("fresh-receiver.db")).expect("open durable receiver");
    for db in [&sender, &receiver] {
        db.execute(DDL, &empty()).expect("declare vector table");
        db.execute(
            "CREATE TABLE filler (id INTEGER PRIMARY KEY) SYNC OFF",
            &empty(),
        )
        .expect("declare row-id filler");
    }
    sender
        .execute(
            "INSERT INTO filler VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)",
            &empty(),
        )
        .expect("skew sender row ids");
    receiver
        .execute("INSERT INTO filler VALUES (1),(2),(3)", &empty())
        .expect("skew receiver row ids differently");
    let before_insert = sender.current_lsn();
    sender
        .execute(
            "INSERT INTO notes VALUES (1, 'deleted-before-replay', '[1,0,0]')",
            &empty(),
        )
        .expect("insert vector owner");
    sender
        .execute("DELETE FROM notes WHERE id = 1", &empty())
        .expect("delete vector owner");
    let retained = sender.changes_since(before_insert);
    assert_eq!(
        retained.rows.iter().filter(|row| !row.deleted).count(),
        1,
        "retained history carries the earlier live owner row"
    );
    assert_eq!(
        retained.rows.iter().filter(|row| row.deleted).count(),
        1,
        "retained history carries the later owner tombstone"
    );
    assert!(
        retained
            .vectors
            .iter()
            .all(|vector| vector.vector.is_empty()),
        "the superseded vector insert is omitted"
    );

    receiver
        .apply_synced_changes(
            retained,
            &sender.conflict_policies(),
            &HashMap::new(),
            SyncAdoption::Continuing,
        )
        .expect("fresh receiver applies retained tombstone history");
    assert!(
        receiver
            .execute("SELECT * FROM notes", &empty())
            .expect("query receiver")
            .rows
            .is_empty(),
        "the owner is absent after replay"
    );
    assert!(
        receiver
            .query_vector(
                VectorIndexRef::new("notes", "embedding"),
                &[1.0, 0.0, 0.0],
                1,
                None,
                receiver.snapshot(),
            )
            .expect("query receiver ANN")
            .is_empty(),
        "the ANN owner is absent after replay"
    );
}

/// A hub assigns its own RowIds, so B's accepted vector delete returns with a
/// remote owner id that is deliberately different from B's former local id.
/// The immediate self-echo must consume only that tombstone group; the second
/// live vector owner proves the cursor did not eat a later group.
#[tokio::test]
async fn deleting_edge_pulls_its_own_vector_tombstone_with_remote_row_id() {
    let tenant = contextdb_core::TenantId::from("vector-self-echo");
    let broker = InProcessBroker::new();
    let hub = Arc::new(Database::open_memory());
    hub.execute(DDL, &empty()).expect("hub table");
    // Occupy hub RowId 1 so B's inserted owners cannot share its ids.
    hub.execute(
        "INSERT INTO notes VALUES (99, 'hub-prefix', '[0,0,1]')",
        &empty(),
    )
    .expect("hub prefix owner");
    let server = Arc::new(SyncServer::with_transport(
        hub.clone(),
        broker.server_as("hub-vector"),
        tenant.clone(),
        ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    ));
    let shutdown = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });

    let edge = Arc::new(Database::open_memory());
    edge.execute(DDL, &empty()).expect("edge table");
    edge.execute(
        "INSERT INTO notes VALUES (1, 'delete-me', '[1,0,0]'), \
         (2, 'survivor', '[0,1,0]')",
        &empty(),
    )
    .expect("edge owners");
    let client = SyncClient::with_transport(edge.clone(), broker.client_as("edge-vector"), tenant);
    client.push().await.expect("seed hub from edge");
    edge.execute("DELETE FROM notes WHERE id = 1", &empty())
        .expect("local vector delete");
    client.push().await.expect("hub accepts delete");
    client
        .pull_default()
        .await
        .expect("immediate vector tombstone self-echo");

    assert!(
        edge.execute("SELECT * FROM notes WHERE id = 1", &empty())
            .expect("deleted-key query")
            .rows
            .is_empty(),
        "the deleted keyed row remains absent"
    );
    assert_eq!(
        edge.execute("SELECT body FROM notes WHERE id = 2", &empty())
            .expect("survivor keyed query")
            .rows,
        vec![vec![Value::Text("survivor".to_string())]],
        "the second keyed vector owner keeps its exact value"
    );
    let deleted_hits = edge
        .query_vector(
            VectorIndexRef::new("notes", "embedding"),
            &[1.0, 0.0, 0.0],
            1,
            None,
            edge.snapshot(),
        )
        .expect("query deleted vector");
    assert!(
        deleted_hits.is_empty() || deleted_hits[0].1 < 0.5,
        "the deleted ANN owner is absent, not mapped through the hub RowId"
    );
    let survivor_hits = edge
        .query_vector(
            VectorIndexRef::new("notes", "embedding"),
            &[0.0, 1.0, 0.0],
            1,
            None,
            edge.snapshot(),
        )
        .expect("query survivor vector");
    assert_eq!(
        survivor_hits.len(),
        1,
        "second vector owner remains indexed"
    );
    assert!(
        survivor_hits[0].1 > 0.99,
        "second vector owner remains intact"
    );

    shutdown.store(true, Ordering::SeqCst);
    let _ = task.await;
}

/// A pulled vector owner (and then its pulled tombstone) is hub-owned outbound
/// history. A local vector edit after that pull is new edge work and must not
/// be suppressed with the old `(table, lsn)` group.
#[tokio::test]
async fn pulled_vector_groups_do_not_echo_but_later_local_vector_work_does() {
    let tenant = contextdb_core::TenantId::from("pulled-vector-outbound");
    let broker = InProcessBroker::new();
    let hub = Arc::new(Database::open_memory());
    hub.execute(DDL, &empty()).expect("hub table");
    hub.execute(
        "INSERT INTO notes VALUES \
         (1, 'hub-vector', '[1,0,0]'), \
         (2, 'hub-delete-later', '[0,0,1]')",
        &empty(),
    )
    .expect("hub vector owners");
    let server = Arc::new(SyncServer::with_transport(
        hub.clone(),
        broker.server_as("hub-pulled-vector"),
        tenant.clone(),
        ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    ));
    let shutdown = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });
    let edge = Arc::new(Database::open_memory());
    edge.execute(DDL, &empty()).expect("edge table");
    let client =
        SyncClient::with_transport(edge.clone(), broker.client_as("edge-pulled-vector"), tenant);
    client.push().await.expect("publish edge schema");
    client.pull_default().await.expect("pull vector owner");
    assert!(
        !client
            .has_pending_push_changes()
            .expect("probe pulled vector"),
        "a pulled row/vector group must produce no outbound echo"
    );

    let before_local_edit = edge.current_lsn();
    edge.execute(
        "UPDATE notes SET embedding = '[0,1,0]' WHERE id = 1",
        &empty(),
    )
    .expect("local vector edit");
    let edited_vector = edge
        .changes_since(before_local_edit)
        .vectors
        .into_iter()
        .find(|vector| !vector.vector.is_empty())
        .expect("local vector edit change");
    assert!(
        !edge.vector_change_arrived_by_sync(&edited_vector),
        "the exact vector edit after a pull remains outbound"
    );
    assert!(
        client
            .has_pending_push_changes()
            .expect("probe local vector edit"),
        "a local edit clears pulled provenance and sends its row/vector group"
    );
    client.push().await.expect("push local vector edit");

    let before_local_delete = edge.current_lsn();
    edge.execute("DELETE FROM notes WHERE id = 1", &empty())
        .expect("local delete after pull");
    let local_delete_vector = edge
        .changes_since(before_local_delete)
        .vectors
        .into_iter()
        .find(|vector| vector.vector.is_empty())
        .expect("local vector tombstone");
    assert!(
        !edge.vector_change_arrived_by_sync(&local_delete_vector),
        "the exact vector delete after a pull remains outbound"
    );
    client.push().await.expect("push local vector delete");

    hub.execute("DELETE FROM notes WHERE id = 2", &empty())
        .expect("hub vector delete");
    client.pull_default().await.expect("pull vector tombstone");
    assert!(
        !client
            .has_pending_push_changes()
            .expect("probe pulled tombstone"),
        "a pulled delete/tombstone vector group must not echo outbound"
    );
    assert!(
        edge.execute("SELECT * FROM notes WHERE id = 2", &empty())
            .expect("deleted row query")
            .rows
            .is_empty(),
        "the pulled tombstone removes the keyed row"
    );
    assert!(
        edge.query_vector(
            VectorIndexRef::new("notes", "embedding"),
            &[0.0, 0.0, 1.0],
            1,
            None,
            edge.snapshot(),
        )
        .expect("deleted vector query")
        .is_empty(),
        "the pulled tombstone removes its ANN owner"
    );
    shutdown.store(true, Ordering::SeqCst);
    let _ = task.await;
}

/// Hub acknowledgement and restored-hub Pending state describe local repair
/// work, not pulled echoes. Their live and delete vectors must both remain
/// outbound.
#[test]
fn accepted_local_and_pending_vector_insert_and_delete_remain_outbound() {
    let db = Database::open_memory();
    db.execute(DDL, &empty()).expect("declare vector table");

    let before_insert = db.current_lsn();
    db.execute(
        "INSERT INTO notes VALUES (7, 'local-vector', '[1,0,0]')",
        &empty(),
    )
    .expect("insert local vector owner");
    let inserted = db.changes_since(before_insert);
    let live_row = inserted
        .rows
        .iter()
        .find(|row| !row.deleted)
        .expect("local live row");
    let live_vector = inserted
        .vectors
        .iter()
        .find(|vector| !vector.vector.is_empty())
        .expect("local live vector");
    db.record_hub_accepted_rows(std::slice::from_ref(live_row), Lsn(800))
        .expect("record AcceptedLocal live owner");
    assert!(
        !db.vector_change_arrived_by_sync(live_vector),
        "an AcceptedLocal live vector remains outbound"
    );
    db.invalidate_accepted_local_ordering_after_hub_regression(before_insert)
        .expect("mark live owner Pending");
    assert!(
        !db.vector_change_arrived_by_sync(live_vector),
        "a Pending live vector remains outbound"
    );

    let before_delete = db.current_lsn();
    db.execute("DELETE FROM notes WHERE id = 7", &empty())
        .expect("delete local vector owner");
    let deleted = db.changes_since(before_delete);
    let delete_row = deleted
        .rows
        .iter()
        .find(|row| row.deleted)
        .expect("local delete row");
    let delete_vector = deleted
        .vectors
        .iter()
        .find(|vector| vector.vector.is_empty())
        .expect("local delete vector");
    db.record_hub_accepted_rows(std::slice::from_ref(delete_row), Lsn(900))
        .expect("record AcceptedLocal delete owner");
    assert!(
        !db.vector_change_arrived_by_sync(delete_vector),
        "an AcceptedLocal delete vector remains outbound"
    );
    db.mark_outbound_rows_pending(before_delete, Some(delete_row.lsn))
        .expect("mark delete owner Pending");
    assert!(
        !db.vector_change_arrived_by_sync(delete_vector),
        "a Pending delete vector remains outbound"
    );
}
