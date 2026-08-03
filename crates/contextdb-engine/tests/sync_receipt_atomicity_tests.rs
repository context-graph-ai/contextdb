use contextdb_core::{Error, Incarnation, Lsn, TenantId, TxId, Value};
use contextdb_engine::{
    Database,
    database::SyncApplyReceipt,
    persistence::RedbPersistence,
    plugin::{CommitSource, DatabasePlugin},
    sync_types::{
        ChangeSet, ConflictPolicies, ConflictPolicy, NaturalKey, RowChange, SyncAdoption,
    },
};
use contextdb_tx::WriteSet;
use std::collections::HashMap;
use std::sync::Arc;

const NOTES_DDL: &str = "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)";

fn pulled_note(id: i64, body: &str, lsn: Lsn) -> ChangeSet {
    ChangeSet {
        rows: vec![RowChange {
            table: "notes".to_string(),
            natural_key: NaturalKey::single("id".to_string(), Value::Int64(id)),
            values: HashMap::from([
                ("id".to_string(), Value::Int64(id)),
                ("body".to_string(), Value::Text(body.to_string())),
            ]),
            deleted: false,
            lsn,
            created_at: None,
        }],
        ..Default::default()
    }
}

fn apply_pulled_note(db: &Database) {
    let source_lsn = Lsn(700);
    db.apply_synced_changes(
        pulled_note(1, "pulled", source_lsn),
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        &HashMap::from([(source_lsn, Some(Lsn(701)))]),
        SyncAdoption::Continuing,
    )
    .expect("apply pulled note");
}

struct RejectSyncedCommit;

impl DatabasePlugin for RejectSyncedCommit {
    fn pre_commit(&self, _ws: &WriteSet, source: CommitSource) -> contextdb_core::Result<()> {
        if source == CommitSource::SyncPull {
            return Err(Error::Other("injected synced-commit failure".to_string()));
        }
        Ok(())
    }
}

#[test]
fn failed_synced_commit_publishes_neither_row_nor_authenticated_receipt() {
    let tenant = TenantId::from("receipt-atomicity");
    let node_id = "edge-receipt-atomicity";
    let incarnation = Incarnation::mint();
    let source = Database::open_memory();
    let params = HashMap::new();
    source
        .execute(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)",
            &params,
        )
        .expect("source table");
    source
        .execute("INSERT INTO notes VALUES (1, 'from-edge')", &params)
        .expect("source row");
    let changes = source.changes_since(Lsn(1));
    let receipt_lsn = changes.max_lsn().expect("row carries a source LSN");

    let temp = tempfile::TempDir::new().expect("tempdir");
    let path = temp.path().join("hub.db");
    let hub = Database::open_with_plugin(&path, Arc::new(RejectSyncedCommit))
        .expect("open rejecting persistent hub");
    hub.execute(
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)",
        &params,
    )
    .expect("hub table");

    let error = hub
        .apply_synced_changes_with_receipt(
            changes,
            &ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists),
            &HashMap::new(),
            SyncAdoption::Continuing,
            SyncApplyReceipt {
                tenant_id: tenant.clone(),
                node_id: node_id.to_string(),
                incarnation,
                source_lsn: receipt_lsn,
                dependency_complete: false,
            },
        )
        .expect_err("injected failure rejects the synced transaction");
    assert!(
        error.to_string().contains("injected synced-commit failure"),
        "the receipt path must expose the failed transaction: {error}"
    );
    assert!(
        hub.persisted_sync_applied_push_watermark_for_node_incarnation(
            &tenant,
            node_id,
            incarnation,
        )
        .expect("read staged receipt")
        .is_none(),
        "a failed sync commit must not publish its authenticated receipt"
    );
    assert_eq!(
        hub.execute("SELECT * FROM notes", &params)
            .expect("query hub after rejected sync")
            .rows
            .len(),
        0,
        "a failed sync commit must not publish its row either"
    );
    hub.close().expect("close hub");

    let reopened = Database::open(&path).expect("reopen hub");
    assert!(
        reopened
            .persisted_sync_applied_push_watermark_for_node_incarnation(
                &tenant,
                node_id,
                incarnation,
            )
            .expect("read receipt after reopen")
            .is_none(),
        "the failed receipt must not appear after reopening the durable hub"
    );
    assert_eq!(
        reopened
            .execute("SELECT * FROM notes", &params)
            .expect("query reopened hub")
            .rows
            .len(),
        0,
        "the failed row must not appear after reopening the durable hub"
    );
}

#[test]
fn durable_table_drop_and_rewrite_cleanup_remove_both_sync_provenance_sidecars() {
    let params = HashMap::new();
    let temp = tempfile::TempDir::new().expect("tempdir");

    let drop_path = temp.path().join("drop.db");
    let drop_db = Database::open(&drop_path).expect("open drop fixture");
    drop_db.execute(NOTES_DDL, &params).expect("create table");
    apply_pulled_note(&drop_db);
    drop_db
        .execute("DROP TABLE notes", &params)
        .expect("drop table");
    drop_db.close().expect("close dropped database");
    let dropped = RedbPersistence::open(&drop_path).expect("open dropped root");
    assert!(
        dropped
            .load_sync_source_lsns()
            .expect("load LSN sidecar")
            .is_empty()
            && dropped
                .load_sync_source_kinds()
                .expect("load kind sidecar")
                .is_empty(),
        "dropping a table must remove both provenance sidecars durably"
    );
    dropped.close();
    let reopened_drop = Database::open(&drop_path).expect("reopen dropped root");
    assert!(
        !reopened_drop
            .table_names()
            .iter()
            .any(|table| table == "notes"),
        "the dropped table must stay absent after reopen"
    );
    reopened_drop.close().expect("close reopened drop root");

    let cleanup_path = temp.path().join("cleanup.db");
    let cleanup_db = Database::open(&cleanup_path).expect("open cleanup fixture");
    cleanup_db
        .execute(NOTES_DDL, &params)
        .expect("create table");
    apply_pulled_note(&cleanup_db);
    cleanup_db.close().expect("close cleanup fixture");
    let cleanup = RedbPersistence::open(&cleanup_path).expect("open cleanup root");
    let mut rows = cleanup
        .load_relational_table("notes")
        .expect("load pulled row for rewrite cleanup");
    rows[0].deleted_tx = Some(TxId(99));
    cleanup
        .rewrite_table_rows("notes", &rows)
        .expect("rewrite cleanup");
    assert!(
        cleanup
            .load_sync_source_lsns()
            .expect("load LSN sidecar")
            .is_empty()
            && cleanup
                .load_sync_source_kinds()
                .expect("load kind sidecar")
                .is_empty(),
        "row cleanup must remove both provenance sidecars in the same rewrite"
    );
    cleanup.close();
    let reopened_cleanup = Database::open(&cleanup_path).expect("reopen cleanup root");
    assert!(
        reopened_cleanup
            .execute("SELECT * FROM notes", &params)
            .expect("query rewritten table")
            .rows
            .is_empty(),
        "the cleanup rewrite must remain applied after reopen"
    );
}

#[test]
fn local_overwrite_clears_durable_accepted_local_provenance() {
    let params = HashMap::new();
    let temp = tempfile::TempDir::new().expect("tempdir");
    let path = temp.path().join("accepted-local.db");
    let db = Database::open(&path).expect("open edge database");
    db.execute(NOTES_DDL, &params).expect("create table");
    db.execute("INSERT INTO notes VALUES (1, 'mine')", &params)
        .expect("insert local row");
    let accepted = db
        .changes_since(Lsn(1))
        .rows
        .into_iter()
        .next()
        .expect("local row change");
    db.record_hub_accepted_rows(std::slice::from_ref(&accepted), Lsn(800), Some("hub"))
        .expect("record hub acceptance");
    let (_, accepted_arrivals) = db.changes_since_with_arrivals(Lsn(0));
    assert_eq!(
        accepted_arrivals.get(&accepted.lsn),
        Some(&Some(Lsn(800))),
        "fixture: acceptance writes an AcceptedLocal companion provenance"
    );

    db.execute(
        "UPDATE notes SET body = 'edited here' WHERE id = 1",
        &params,
    )
    .expect("local overwrite");
    let (changes, arrivals) = db.changes_since_with_arrivals(Lsn(0));
    let current = changes
        .rows
        .iter()
        .find(|row| row.natural_key.value == Value::Int64(1))
        .expect("current row change");
    assert_eq!(
        arrivals.get(&current.lsn),
        Some(&None),
        "a local overwrite must clear AcceptedLocal's LSN and kind together"
    );
    db.close().expect("close overwritten database");
    let persisted = RedbPersistence::open(&path).expect("open overwritten root");
    assert!(
        persisted
            .load_sync_source_lsns()
            .expect("load LSN sidecar")
            .is_empty()
            && persisted
                .load_sync_source_kinds()
                .expect("load kind sidecar")
                .is_empty(),
        "the local overwrite must clear both sidecars durably before reopen"
    );
    persisted.close();
}

#[test]
fn status_regression_keeps_live_and_delete_work_pending_until_fresh_acknowledgement() {
    let params = HashMap::new();
    let temp = tempfile::TempDir::new().expect("tempdir");
    let path = temp.path().join("pending-reorder.db");
    let db = Database::open(&path).expect("open edge database");
    db.execute(NOTES_DDL, &params).expect("create table");
    db.execute("INSERT INTO notes VALUES (1, 'lost-local')", &params)
        .expect("insert lost local row");
    let local = db
        .changes_since(Lsn(1))
        .rows
        .into_iter()
        .next()
        .expect("local row change");
    db.record_hub_accepted_rows(std::slice::from_ref(&local), Lsn(800), Some("hub"))
        .expect("record old hub acknowledgement");
    db.invalidate_accepted_local_ordering_after_hub_regression(Lsn(1))
        .expect("mark restored-hub row pending fresh ordering");
    let (_, pending_arrivals) = db.changes_since_with_arrivals(Lsn(0));
    assert_eq!(
        pending_arrivals.get(&local.lsn),
        Some(&None),
        "a restored-hub resend must not carry its discarded AcceptedLocal arrival"
    );
    let stale = pulled_note(1, "restored-stale", Lsn(77));
    db.apply_synced_changes(
        stale,
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        &HashMap::from([(Lsn(77), Some(Lsn(900)))]),
        SyncAdoption::Continuing,
    )
    .expect("pending row blocks same-source pull");
    assert_eq!(
        db.execute("SELECT body FROM notes WHERE id = 1", &params)
            .expect("query pending row")
            .rows,
        vec![vec![Value::Text("lost-local".to_string())]],
        "a continuing pull cannot overwrite local work while its hub order is pending"
    );
    db.close().expect("close pending edge");

    let reopened = Database::open(&path).expect("reopen pending edge");
    let (_, reopened_arrivals) = reopened.changes_since_with_arrivals(Lsn(0));
    assert_eq!(
        reopened_arrivals.get(&local.lsn),
        Some(&None),
        "the pending reorder state must survive reopen before resend"
    );
    reopened
        .record_hub_accepted_rows(std::slice::from_ref(&local), Lsn(51), Some("hub"))
        .expect("fresh restored-hub acknowledgement");
    let (_, refreshed_arrivals) = reopened.changes_since_with_arrivals(Lsn(0));
    assert_eq!(
        refreshed_arrivals.get(&local.lsn),
        Some(&Some(Lsn(51))),
        "reacceptance must replace pending state with the restored hub's new order"
    );
}

#[test]
fn confirmed_identical_echo_stays_pending_across_reopen_until_refresh_commits() {
    let params = HashMap::new();
    let temp = tempfile::TempDir::new().expect("tempdir");
    let path = temp.path().join("confirmed-echo-crash.db");
    let db = Database::open(&path).expect("open edge database");
    db.execute(NOTES_DDL, &params).expect("create table");
    db.execute("INSERT INTO notes VALUES (1, 'local')", &params)
        .expect("local write");
    let local = db
        .changes_since(Lsn(1))
        .rows
        .into_iter()
        .next()
        .expect("local change");
    db.mark_outbound_rows_pending(Lsn(1), Some(local.lsn))
        .expect("status-confirmed batch becomes Pending before pull");
    db.apply_synced_changes(
        pulled_note(1, "local", local.lsn),
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        &HashMap::from([(local.lsn, Some(Lsn(901)))]),
        SyncAdoption::ConfirmedPendingReconciliation,
    )
    .expect("apply identical confirmed echo");
    // Deliberately do not call refresh_confirmed_pending_rows: this is the
    // crash boundary between apply and its follow-up provenance refresh.
    db.close()
        .expect("close at apply-to-refresh crash boundary");

    let reopened = Database::open(&path).expect("reopen after simulated crash");
    let (_, arrivals) = reopened.changes_since_with_arrivals(Lsn(0));
    assert_eq!(
        arrivals.get(&local.lsn),
        Some(&None),
        "the durable row remains Pending rather than becoming Pulled at the crash boundary"
    );
    let stale = pulled_note(1, "restored-stale", Lsn(77));
    reopened
        .apply_synced_changes(
            stale,
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
            &HashMap::from([(Lsn(77), Some(Lsn(900)))]),
            SyncAdoption::Continuing,
        )
        .expect("ordinary restored pull is blocked after reopen");
    assert_eq!(
        reopened
            .execute("SELECT body FROM notes WHERE id = 1", &params)
            .expect("query protected row")
            .rows,
        vec![vec![Value::Text("local".to_string())]],
        "a restored hub cannot overwrite the unrefreshed local row"
    );
}

#[test]
fn status_regression_keeps_accepted_local_delete_pushable_and_unordered() {
    let params = HashMap::new();
    let edge = Database::open_memory();
    edge.execute(NOTES_DDL, &params).expect("create table");
    edge.execute("INSERT INTO notes VALUES (1, 'delete-me')", &params)
        .expect("insert row");
    let live = edge
        .changes_since(Lsn(1))
        .rows
        .into_iter()
        .next()
        .expect("live row");
    let live_lsn = live.lsn;
    edge.execute("DELETE FROM notes WHERE id = 1", &params)
        .expect("delete row");
    let delete = edge
        .changes_since(live_lsn)
        .rows
        .into_iter()
        .find(|row| row.deleted)
        .expect("delete row change");
    edge.record_hub_accepted_rows(std::slice::from_ref(&delete), Lsn(800), Some("hub"))
        .expect("record old delete acknowledgement");
    edge.invalidate_accepted_local_ordering_after_hub_regression(Lsn(1))
        .expect("invalidate old delete order");
    let (_, arrivals) = edge.changes_since_with_arrivals(Lsn(0));
    assert_eq!(
        arrivals.get(&delete.lsn),
        Some(&None),
        "the lost delete resends without the discarded hub order"
    );
    assert!(
        !edge.row_change_arrived_by_sync(&delete),
        "an invalidated AcceptedLocal delete remains outbound work"
    );
    edge.apply_synced_changes(
        ChangeSet {
            rows: vec![live],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        &HashMap::from([(live_lsn, Some(Lsn(900)))]),
        SyncAdoption::Continuing,
    )
    .expect("invalidated tombstone blocks stale pull");
    assert!(
        edge.execute("SELECT * FROM notes", &params)
            .expect("query after stale live pull")
            .rows
            .is_empty(),
        "a stale pull cannot resurrect a delete before its fresh acknowledgement"
    );
    edge.refresh_confirmed_pending_rows(
        std::slice::from_ref(&delete),
        &HashMap::from([(delete.lsn, Some(Lsn(901)))]),
    )
    .expect("confirmed delete echo refreshes its invalidated tombstone");
    let (_, refreshed_arrivals) = edge.changes_since_with_arrivals(Lsn(0));
    assert_eq!(
        refreshed_arrivals.get(&delete.lsn),
        Some(&Some(Lsn(901))),
        "a confirmed delete echo replaces the discarded hub order"
    );
    edge.apply_synced_changes(
        pulled_note(1, "later-hub-edit", Lsn(78)),
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        &HashMap::from([(Lsn(78), Some(Lsn(902)))]),
        SyncAdoption::Continuing,
    )
    .expect("later hub edit follows confirmed delete");
    assert_eq!(
        edge.execute("SELECT body FROM notes WHERE id = 1", &params)
            .expect("query after later hub edit")
            .rows,
        vec![vec![Value::Text("later-hub-edit".to_string())]],
        "a fresh delete acknowledgement must not suppress later hub truth"
    );
}

#[test]
fn restored_before_pending_marks_a_fresh_local_delete_stampless_until_confirmed() {
    let params = HashMap::new();
    let edge = Database::open_memory();
    edge.execute(NOTES_DDL, &params).expect("create table");
    edge.execute("INSERT INTO notes VALUES (1, 'delete-me')", &params)
        .expect("insert row");
    let before_delete = edge.current_lsn();
    edge.execute("DELETE FROM notes WHERE id = 1", &params)
        .expect("fresh local delete");
    let delete = edge
        .changes_since(before_delete)
        .rows
        .into_iter()
        .find(|row| row.deleted)
        .expect("current delete change");
    edge.mark_outbound_rows_pending(before_delete, Some(delete.lsn))
        .expect("mark fresh delete Pending before restored-hub pull");
    let (_, arrivals) = edge.changes_since_with_arrivals(Lsn(0));
    assert_eq!(
        arrivals.get(&delete.lsn),
        Some(&None),
        "a fresh Pending delete must not present its local LSN as a hub arrival"
    );
    edge.apply_synced_changes(
        pulled_note(1, "restored-stale", Lsn(77)),
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        &HashMap::from([(Lsn(77), Some(Lsn(900)))]),
        SyncAdoption::Continuing,
    )
    .expect("Pending delete blocks ordinary restored pull");
    assert!(
        edge.execute("SELECT * FROM notes", &params)
            .expect("query after stale pull")
            .rows
            .is_empty(),
        "the restored hub cannot resurrect a fresh local delete before confirmation"
    );
    edge.refresh_confirmed_pending_rows(
        std::slice::from_ref(&delete),
        &HashMap::from([(delete.lsn, Some(Lsn(901)))]),
    )
    .expect("confirmed delete echo refreshes Pending tombstone");
    edge.apply_synced_changes(
        pulled_note(1, "later-hub-edit", Lsn(78)),
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        &HashMap::from([(Lsn(78), Some(Lsn(902)))]),
        SyncAdoption::Continuing,
    )
    .expect("later hub edit follows fresh delete acknowledgement");
    assert_eq!(
        edge.execute("SELECT body FROM notes WHERE id = 1", &params)
            .expect("query later edit")
            .rows,
        vec![vec![Value::Text("later-hub-edit".to_string())]],
        "the confirmed tombstone must release later hub truth"
    );
}
