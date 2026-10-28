use super::*;
include!("durable_test_observations.rs");

fn create_table() -> DdlChange {
    DdlChange::CreateTable {
        name: "events".to_string(),
        columns: vec![("id".to_string(), "INTEGER PRIMARY KEY".to_string())],
        constraints: Vec::new(),
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    }
}

fn create_trigger() -> DdlChange {
    DdlChange::CreateTrigger {
        name: "events_audit".to_string(),
        table: "events".to_string(),
        on_events: vec!["INSERT".to_string()],
    }
}

fn alter_table() -> DdlChange {
    DdlChange::AlterTable {
        name: "events".to_string(),
        columns: vec![
            ("id".to_string(), "INTEGER PRIMARY KEY".to_string()),
            ("body".to_string(), "TEXT".to_string()),
        ],
        constraints: Vec::new(),
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    }
}

fn received_context() -> crate::protocol::ReceivedDdlContext {
    crate::protocol::ReceivedDdlContext {
        tenant_id: TenantId::from("tenant-a"),
        source_node_id: "authenticated-source".to_string(),
        source_incarnation: Incarnation(7),
        entries: vec![
            crate::protocol::ReceivedDdlEntry {
                source_ddl_lsn: Lsn(41),
                ordinal: 0,
                table: Some("events".to_string()),
                table_generation: Some(3),
                digest: vec![1],
                author: None,
            },
            crate::protocol::ReceivedDdlEntry {
                source_ddl_lsn: Lsn(41),
                ordinal: 1,
                table: Some("events".to_string()),
                table_generation: Some(3),
                digest: vec![2],
                author: None,
            },
            crate::protocol::ReceivedDdlEntry {
                source_ddl_lsn: Lsn(41),
                ordinal: 2,
                table: Some("events".to_string()),
                table_generation: Some(3),
                digest: vec![3],
                author: None,
            },
        ],
    }
}

fn lineage(author: &str, position: u64, generation: u64) -> crate::protocol::WireRowLineage {
    crate::protocol::WireRowLineage {
        author_node_id: author.to_string(),
        author_database_incarnation: Incarnation(17),
        author_local_mutation_position: Lsn(position),
        table_generation: generation,
        lineage_root: format!("author:{author}:{}:{position}", Incarnation(17).to_hex()),
        attestation: vec![0x7f],
    }
}

#[test]
fn internal_sync_and_prepared_replacement_refuse_legacy_null_primary_key() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE guarded_state (id UUID, tenant_id UUID, state TEXT, PRIMARY KEY (id, tenant_id)) STATE MACHINE (state: pending -> [done])",
        &HashMap::new(),
    )
    .unwrap();
    let id = uuid::Uuid::new_v4();
    let legacy_values = HashMap::from([
        ("id".to_string(), Value::Uuid(id)),
        ("tenant_id".to_string(), Value::Null),
        ("state".to_string(), Value::Text("pending".to_string())),
    ]);

    let tx = db.begin().unwrap();
    let fixture_operation_reader_1 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    assert!(
        db.insert_synced_row_at(
            tx,
            "guarded_state",
            RowId(1),
            legacy_values.clone(),
            db.snapshot(),
            None,
            fixture_operation_reader_1.clone()
        )
        .is_err(),
        "the final synced-row writer must refuse a NULL primary-key member"
    );
    db.rollback(tx).unwrap();

    let existing = VersionedRow {
        row_id: RowId(2),
        values: legacy_values.clone(),
        created_tx: TxId(1),
        deleted_tx: None,
        lsn: Lsn(1),
        created_at: None,
    };
    let mut next_values = legacy_values;
    next_values.insert("state".to_string(), Value::Text("done".to_string()));
    let mut write_set = WriteSet::default();
    let mut options = PreparedPropagationOptions {
        tx: TxId(2),
        snapshot: db.snapshot(),
        lookup_delta: None,
    };
    let fixture_operation_reader_2 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    assert!(
        db.upsert_row_in_prepared_write_set(
            &mut write_set,
            "guarded_state",
            "state",
            &existing,
            next_values,
            &mut options,
            fixture_operation_reader_2.clone()
        )
        .is_err(),
        "prepared state propagation must not republish a legacy NULL primary key"
    );
    assert!(write_set.relational_inserts.is_empty());
    assert!(write_set.relational_deletes.is_empty());
}

#[test]
fn received_schema_source_order_keeps_create_trigger_alter_exactly_as_authored() {
    let ddl = vec![create_table(), create_trigger(), alter_table()];
    let (order, payload) = plan_received_schema_source_order(
        Lsn(9),
        &ddl,
        &[Lsn(41), Lsn(41), Lsn(41)],
        Some(&received_context()),
    )
    .expect("source-order plan");

    assert_eq!(
        order.iter().map(|entry| &entry.change).collect::<Vec<_>>(),
        ddl.iter().collect::<Vec<_>>(),
        "the planner must not regroup trigger DDL ahead of or behind table DDL"
    );
    assert_eq!(
        payload
            .ddl_log_replacement
            .iter()
            .map(|(_, change)| change)
            .collect::<Vec<_>>(),
        ddl.iter().collect::<Vec<_>>(),
        "the durable DDL payload preserves create-table → trigger → alter-table"
    );
    assert_eq!(
        order
            .iter()
            .map(|entry| entry.persisted_ordinal)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(payload.generation_values.len(), 3);
    assert_eq!(payload.marker_values.len(), 3);
}

// ---- Authenticated journeys between real peers ------------------------
//
// The proofs below move a unit between peers exactly as sync does: the
// sender extracts what it committed (`checked_changes_since_with_arrivals`),
// signs each row's creator lineage with its fabric identity, attaches the
// source-owned DDL provenance, and the receiver takes delivery through the
// same validation and apply calls the hub push handler and the pull client
// make. Nothing is hand-built: every identity asserted on is one a peer
// produced.

const JOURNEY_TENANT: &str = "authenticated-journey-tenant";

struct JourneyPeer {
    db: Database,
    path: std::path::PathBuf,
    identity: Arc<crate::identity::FabricIdentity>,
}

impl JourneyPeer {
    fn open(root: &std::path::Path, name: &str) -> Self {
        let path = root.join(format!("{name}.db"));
        let identity = Arc::new(
            crate::identity::FabricIdentity::load_or_generate(&root.join(format!("{name}.key")))
                .expect("persist the peer's fabric identity"),
        );
        Self {
            db: Database::open(&path).expect("open the peer"),
            path,
            identity,
        }
    }

    fn node_id(&self) -> String {
        self.identity.node_id()
    }

    fn incarnation(&self) -> Incarnation {
        self.db
            .sync_incarnation(&TenantId::from(JOURNEY_TENANT))
            .expect("the peer's database incarnation")
    }

    fn reopen(self) -> Self {
        self.db.close().expect("close the peer");
        drop(self.db);
        Self {
            db: Database::open(&self.path).expect("reopen the peer"),
            path: self.path,
            identity: self.identity,
        }
    }

    fn persisted<T: serde::de::DeserializeOwned>(&self, key: &str) -> Option<T> {
        self.db
            .persistence
            .as_ref()
            .expect("a file-backed peer")
            .load_config_value::<T>(key)
            .expect("read the peer's durable store")
    }
}

/// Everything `sender` committed after `since`, in the wire form a sync
/// sender puts on the network: its extraction, each row's signed creator
/// lineage, and the schema provenance for each DDL occurrence.
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn served_by(sender: &JourneyPeer, since: Lsn) -> crate::protocol::WireChangeSet {
    let tenant = TenantId::from(JOURNEY_TENANT);
    let fixture_operation_reader_1 = sender
        .db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");
    let mut fixture_operation_copy_1 =
        contextdb_engine::vector_read_memory::admission(fixture_operation_reader_1.clone());
    let mut fixture_selection_credit_1 =
        contextdb_core::read_memory::ReadCredit::new(fixture_operation_reader_1.clone());

    let (changes, arrivals, source) = sender
        .db
        .checked_changes_since_with_arrivals(
            since,
            &mut fixture_operation_copy_1,
            fixture_operation_reader_1.clone(),
            &mut fixture_selection_credit_1,
            None,
        )
        .expect("the sender extracts what it committed");
    let lineages = if changes.rows.is_empty() {
        HashMap::new()
    } else {
        let fixture_operation_reader_2 = sender
            .db
            .declared_reader_memory()
            .expect("admit the actual fixture reader");

        sender
            .db
            .outbound_row_lineages(
                &changes,
                &tenant,
                &sender.node_id(),
                sender.incarnation(),
                &|bytes| Ok(sender.identity.sign_lineage(bytes)),
                fixture_operation_reader_2.clone(),
            )
            .expect("the sender attaches each row's signed creator lineage")
    };
    let fixture_operation_reader_3 = sender
        .db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    let provenance = sender
        .db
        .outbound_ddl_provenance(
            &changes,
            &source,
            &tenant,
            &sender.node_id(),
            sender.incarnation(),
            &|bytes| Ok(sender.identity.sign_lineage(bytes)),
            fixture_operation_reader_3.clone(),
        )
        .expect("the sender attaches each DDL occurrence's signed provenance");
    crate::protocol::wire_changeset_with_arrivals_lineages_and_ddl_provenance(
        changes, &arrivals, &lineages, provenance,
    )
}

#[test]
fn refused_schema_outcome_is_durable_exact_and_not_an_acceptance_receipt() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let author = JourneyPeer::open(root.path(), "policy-author");
    let hub = JourneyPeer::open(root.path(), "policy-hub");
    let sql =
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) SYNC TWO WAY SYNC CONFLICT KEEP FIRST";
    author.db.execute(sql, &HashMap::new()).unwrap();
    hub.db.execute(sql, &HashMap::new()).unwrap();
    pushed_to(&hub, &author, served_by(&author, Lsn(0)));
    let base = author.db.current_lsn();
    author
        .db
        .execute(
            "ALTER TABLE notes SET SYNC CONFLICT KEEP LATEST",
            &HashMap::new(),
        )
        .unwrap();
    let wire = served_by(&author, base);
    let first_digest = crate::protocol::authored_schema_unit_digest(&[wire.ddl_provenance[0]
        .author
        .as_ref()
        .unwrap()])
    .unwrap();
    let tenant = TenantId::from(JOURNEY_TENANT);
    let before = hub
        .db
        .persisted_sync_applied_push_watermark_for_node_incarnation(
            &tenant,
            &author.node_id(),
            author.incarnation(),
        )
        .unwrap();
    let refused = pushed_to(&hub, &author, wire.clone());
    assert_eq!(refused.result.conflicts.len(), 1);
    let fixture_operation_reader_4 = hub
        .db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    let (outcomes, _outcome_lease) = hub
        .db
        .refused_schema_unit_outcomes_for_digests(
            &tenant,
            &author.node_id(),
            author.incarnation(),
            &hub.node_id(),
            &[first_digest],
            fixture_operation_reader_4.clone(),
        )
        .unwrap();
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].author_node_id, author.node_id());
    assert_eq!(outcomes[0].source_ddl_lsn, author.db.current_lsn());
    assert_eq!(outcomes[0].hub_node_id, hub.node_id());
    assert!(
        hub.db
            .replay_sync_push_outcome(&refused.outcome_key)
            .unwrap()
            .is_none()
    );
    let hub = hub.reopen();
    hub.db
        .execute(
            "ALTER TABLE notes SET SYNC CONFLICT KEEP LATEST",
            &HashMap::new(),
        )
        .unwrap();
    let retried = pushed_to(&hub, &author, wire);
    assert_eq!(
        retried.result.conflicts[0].reason.as_deref(),
        Some(outcomes[0].reason.as_str())
    );
    let fixture_operation_reader_5 = hub
        .db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    assert_eq!(
        hub.db
            .refused_schema_unit_outcomes_for_digests(
                &tenant,
                &author.node_id(),
                author.incarnation(),
                &hub.node_id(),
                &[first_digest],
                fixture_operation_reader_5.clone()
            )
            .unwrap()
            .0,
        outcomes
    );
    assert_eq!(
        hub.db
            .persisted_sync_applied_push_watermark_for_node_incarnation(
                &tenant,
                &author.node_id(),
                author.incarnation()
            )
            .unwrap(),
        before
    );
    let fixture_operation_reader_6 = hub
        .db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    assert!(
        hub.db
            .refused_schema_unit_outcomes_for_digests(
                &tenant,
                &author.node_id(),
                Incarnation(123),
                &hub.node_id(),
                &[first_digest],
                fixture_operation_reader_6.clone()
            )
            .unwrap()
            .0
            .is_empty()
    );
    author
        .db
        .execute(
            "CREATE TABLE sibling (id UUID PRIMARY KEY) SYNC TWO WAY",
            &HashMap::new(),
        )
        .unwrap();
    let mixed_retry = pushed_to(&hub, &author, served_by(&author, base));
    assert_eq!(mixed_retry.result.conflicts.len(), 1);
    assert!(hub.db.table_meta("sibling").is_some());
    hub.db
        .execute(
            "ALTER TABLE notes SET SYNC CONFLICT KEEP FIRST",
            &HashMap::new(),
        )
        .unwrap();
    let refused_lsn = author.db.current_lsn();
    author
        .db
        .execute(
            "ALTER TABLE notes SET SYNC CONFLICT KEEP FIRST",
            &HashMap::new(),
        )
        .unwrap();
    assert!(
        pushed_to(&hub, &author, served_by(&author, refused_lsn))
            .result
            .conflicts
            .is_empty()
    );
    let corrected_lsn = author.db.current_lsn();
    author
        .db
        .execute(
            "ALTER TABLE notes SET SYNC CONFLICT KEEP LATEST",
            &HashMap::new(),
        )
        .unwrap();
    let later_wire = served_by(&author, corrected_lsn);
    let second_digest = crate::protocol::authored_schema_unit_digest(&[later_wire.ddl_provenance
        [0]
    .author
    .as_ref()
    .unwrap()])
    .unwrap();
    pushed_to(&hub, &author, later_wire);
    let fixture_operation_reader_7 = hub
        .db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    let (both, _both_lease) = hub
        .db
        .refused_schema_unit_outcomes_for_digests(
            &tenant,
            &author.node_id(),
            author.incarnation(),
            &hub.node_id(),
            &[first_digest, second_digest],
            fixture_operation_reader_7.clone(),
        )
        .unwrap();
    assert_eq!(both.len(), 2);
    assert_ne!(both[0].unit_digest, both[1].unit_digest);
}

#[test]
fn policy_held_data_keeps_evidence_and_replays_once_after_restart() {
    prove_policy_held_replay(false);
}

#[test]
fn connected_policy_held_data_replays_as_one_complete_source_unit() {
    prove_policy_held_replay(true);
}

fn prove_policy_held_replay(dependency_complete: bool) {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let author = JourneyPeer::open(root.path(), "held-author");
    let hub = JourneyPeer::open(root.path(), "held-hub");
    let sql =
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) SYNC TWO WAY SYNC CONFLICT KEEP FIRST";
    author.db.execute(sql, &HashMap::new()).unwrap();
    hub.db.execute(sql, &HashMap::new()).unwrap();
    pushed_to(&hub, &author, served_by(&author, Lsn(0)));
    let base = author.db.current_lsn();
    author
        .db
        .execute(
            "ALTER TABLE notes SET SYNC CONFLICT KEEP LATEST",
            &HashMap::new(),
        )
        .unwrap();
    assert_eq!(
        pushed_to(&hub, &author, served_by(&author, base))
            .result
            .conflicts
            .len(),
        1
    );
    let refused = author.db.current_lsn();
    author
        .db
        .execute(
            "INSERT INTO notes VALUES ('00000000-0000-0000-0000-000000000042', 'pending')",
            &HashMap::new(),
        )
        .unwrap();
    if dependency_complete {
        author
            .db
            .execute(
                "INSERT INTO notes VALUES ('00000000-0000-0000-0000-000000000043', 'pending')",
                &HashMap::new(),
            )
            .unwrap();
    }
    let expected_rows = if dependency_complete { 2 } else { 1 };
    let row_wire = served_by(&author, refused);
    if dependency_complete {
        assert_ne!(row_wire.rows[0].lsn, row_wire.rows[1].lsn);
    }
    let expected_lineages = crate::protocol::wire_row_lineages(&row_wire);
    let receipt_before = hub
        .db
        .persisted_sync_applied_push_watermark_for_node_incarnation(
            &TenantId::from(JOURNEY_TENANT),
            &author.node_id(),
            author.incarnation(),
        )
        .unwrap();
    let held = try_pushed_to_with_atomicity(&hub, &author, row_wire, dependency_complete).unwrap();
    assert_eq!(held.result.applied_rows, 0);
    assert_eq!(
        hub.db
            .persisted_sync_applied_push_watermark_for_node_incarnation(
                &TenantId::from(JOURNEY_TENANT),
                &author.node_id(),
                author.incarnation()
            )
            .unwrap(),
        receipt_before
    );
    assert!(
        hub.db
            .execute("SELECT body FROM notes", &HashMap::new())
            .unwrap()
            .rows()
            .is_empty()
    );
    let records = hub
        .db
        .persistence
        .as_ref()
        .unwrap()
        .load_config_values_with_prefix::<DurablePolicyHeldData>(Database::POLICY_HELD_DATA_PREFIX)
        .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].1.lineages, expected_lineages);
    assert_eq!(records[0].1.changes.rows.len(), expected_rows);
    assert_eq!(
        records[0].1.receipt.dependency_complete,
        dependency_complete
    );
    let stale_holds = hub.db.policy_holds().unwrap();
    assert_eq!(stale_holds.len(), 1);
    let row_lsn = author.db.current_lsn();
    let hub = hub.reopen();
    author
        .db
        .execute(
            "ALTER TABLE notes SET SYNC CONFLICT KEEP FIRST",
            &HashMap::new(),
        )
        .unwrap();
    pushed_to(&hub, &author, served_by(&author, row_lsn));
    // Simulate a crash after the correction commit but before hold cleanup.
    for hold in stale_holds {
        hub.db
            .persistence
            .as_ref()
            .unwrap()
            .flush_config_value(
                &Database::policy_hold_key(&hold.source_node_id, &hold.table),
                &hold,
            )
            .unwrap();
    }
    let hub = hub.reopen();
    assert!(hub.db.policy_held_received_tables().unwrap().is_empty());
    let tenant = TenantId::from(JOURNEY_TENANT);
    let fixture_operation_reader_8 = hub
        .db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");
    let mut fixture_operation_copy_8 =
        contextdb_engine::vector_read_memory::admission(fixture_operation_reader_8.clone());

    let replayed = hub
        .db
        .replay_released_policy_data(
            &tenant,
            &author.node_id(),
            author.incarnation(),
            &mut fixture_operation_copy_8,
            fixture_operation_reader_8.clone(),
        )
        .unwrap();
    assert_eq!(replayed.applied_rows, expected_rows);
    assert_eq!(
        hub.db
            .execute("SELECT body FROM notes", &HashMap::new())
            .unwrap()
            .rows(),
        vec![vec![Value::Text("pending".into())]; expected_rows]
    );
    // Recreate the crash window: the applied payload survived cleanup.
    let (key, pending) = &records[0];
    hub.db
        .persistence
        .as_ref()
        .unwrap()
        .flush_config_value(key, pending)
        .unwrap();
    let before_retry = hub.db.current_lsn();
    let hub = hub.reopen();
    let fixture_operation_reader_9 = hub
        .db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");
    let mut fixture_operation_copy_9 =
        contextdb_engine::vector_read_memory::admission(fixture_operation_reader_9.clone());

    hub.db
        .replay_released_policy_data(
            &tenant,
            &author.node_id(),
            author.incarnation(),
            &mut fixture_operation_copy_9,
            fixture_operation_reader_9.clone(),
        )
        .unwrap();
    assert_eq!(hub.db.current_lsn(), before_retry);
    assert_eq!(
        hub.db
            .execute("SELECT body FROM notes", &HashMap::new())
            .unwrap()
            .rows()
            .len(),
        expected_rows
    );
    assert!(
        hub.db
            .persistence
            .as_ref()
            .unwrap()
            .load_config_values_with_prefix::<DurablePolicyHeldData>(
                Database::POLICY_HELD_DATA_PREFIX
            )
            .unwrap()
            .is_empty()
    );
}

struct PushedUnit {
    result: ApplyResult,
    /// The authenticated schema context, when the request carried schema.
    received: Option<crate::protocol::ReceivedDdlContext>,
    outcome_key: SyncPushOutcomeKey,
    receipt: SyncApplyReceipt,
}

/// `sender` pushes `wire` to the hub `receiver`, which takes delivery the
/// way the hub push handler does: authenticated schema context from the
/// sender's identity, lineage validation, replay rejection, then the
/// receipted apply that keeps a durable per-request outcome.
fn pushed_to(
    receiver: &JourneyPeer,
    sender: &JourneyPeer,
    wire: crate::protocol::WireChangeSet,
) -> PushedUnit {
    try_pushed_to(receiver, sender, wire)
        .unwrap_or_else(|error| panic!("the hub installs the authenticated unit whole: {error}"))
}

/// [`pushed_to`] through the same door, handing back the receiver's
/// refusal instead of treating it as a fixture failure.
fn try_pushed_to(
    receiver: &JourneyPeer,
    sender: &JourneyPeer,
    wire: crate::protocol::WireChangeSet,
) -> std::result::Result<PushedUnit, String> {
    try_pushed_to_with_atomicity(receiver, sender, wire, false)
}

fn try_pushed_to_with_atomicity(
    receiver: &JourneyPeer,
    sender: &JourneyPeer,
    wire: crate::protocol::WireChangeSet,
    dependency_complete: bool,
) -> std::result::Result<PushedUnit, String> {
    let tenant = TenantId::from(JOURNEY_TENANT);
    let node = sender.node_id();
    let incarnation = sender.incarnation();
    let request = crate::protocol::PushRequest {
        changeset: wire,
        incarnation,
    };
    let outcome_key =
        Database::sync_push_outcome_key(&tenant, &node, &request, dependency_complete)
            .expect("the hub keys this exact request's outcome");
    let received =
        crate::protocol::received_ddl_context(&request.changeset, &tenant, &node, incarnation)
            .map_err(|error| format!("{error:?}"))?;
    let arrivals = crate::protocol::wire_row_arrivals(&request.changeset);
    let lineages = crate::protocol::wire_row_lineages(&request.changeset);
    let changes = ChangeSet::try_from(request.changeset).map_err(|error| format!("{error:?}"))?;
    let changes = if let Some(received) = received.as_ref() {
        let fixture_operation_reader_10 = receiver
            .db
            .declared_reader_memory()
            .expect("admit the actual fixture reader");

        receiver
            .db
            .validate_incoming_push_lineages_with_received_ddl(
                &tenant,
                &changes,
                &lineages,
                &node,
                incarnation,
                received,
                fixture_operation_reader_10.clone(),
            )
            .map_err(|error| format!("{error:?}"))?;
        let fixture_operation_reader_11 = receiver
            .db
            .declared_reader_memory()
            .expect("admit the actual fixture reader");

        receiver
            .db
            .reject_accepted_lineage_replays_with_received_ddl(
                &tenant,
                changes,
                &lineages,
                received,
                fixture_operation_reader_11.clone(),
            )
            .map_err(|error| format!("{error:?}"))?
    } else {
        let fixture_operation_reader_12 = receiver
            .db
            .declared_reader_memory()
            .expect("admit the actual fixture reader");

        receiver
            .db
            .validate_incoming_push_lineages(
                &tenant,
                &changes,
                &lineages,
                &node,
                incarnation,
                fixture_operation_reader_12.clone(),
            )
            .map_err(|error| format!("{error:?}"))?;
        let fixture_operation_reader_13 = receiver
            .db
            .declared_reader_memory()
            .expect("admit the actual fixture reader");

        receiver
            .db
            .reject_accepted_lineage_replays(
                &tenant,
                changes,
                &lineages,
                fixture_operation_reader_13.clone(),
            )
            .map_err(|error| format!("{error:?}"))?
    };
    let receipt = SyncApplyReceipt {
        tenant_id: tenant,
        node_id: node,
        incarnation,
        source_lsn: changes.max_lsn().expect("the unit has a source frontier"),
        dependency_complete,
        hub_contact_ms: None,
    };
    let fixture_operation_reader_14 = receiver
        .db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");
    let mut fixture_operation_copy_14 =
        contextdb_engine::vector_read_memory::admission(fixture_operation_reader_14.clone());

    let result = receiver
        .db
        .apply_authenticated_received_changes_with_outcome(
            changes,
            &arrivals,
            SyncAdoption::Continuing,
            receipt.clone(),
            Some(&receiver.node_id()),
            &lineages,
            received.as_ref(),
            Some(&outcome_key),
            &mut fixture_operation_copy_14,
            fixture_operation_reader_14.clone(),
        )
        .map_err(|error| format!("{error:?}"))?;
    Ok(PushedUnit {
        result,
        received,
        outcome_key,
        receipt,
    })
}

/// `receiver` pulls `wire` from the hub `hub` and applies it the way the
/// pull client does: authenticated schema context naming the hub, lineage
/// validation against the received schema, then the authenticated apply.
fn pulled_from(
    receiver: &JourneyPeer,
    hub: &JourneyPeer,
    wire: crate::protocol::WireChangeSet,
) -> crate::protocol::ReceivedDdlContext {
    try_pulled_from(receiver, hub, wire)
        .unwrap_or_else(|error| panic!("the edge installs the served unit whole: {error}"))
}

/// [`pulled_from`] through the same door, handing back the receiver's
/// refusal instead of treating it as a fixture failure.
fn try_pulled_from(
    receiver: &JourneyPeer,
    hub: &JourneyPeer,
    wire: crate::protocol::WireChangeSet,
) -> std::result::Result<crate::protocol::ReceivedDdlContext, String> {
    let tenant = TenantId::from(JOURNEY_TENANT);
    let received =
        crate::protocol::received_ddl_context(&wire, &tenant, &hub.node_id(), hub.incarnation())
            .map_err(|error| format!("{error:?}"))?
            .expect("the served unit carries schema");
    let arrivals = crate::protocol::wire_row_arrivals(&wire);
    let lineages = crate::protocol::wire_row_lineages(&wire);
    let changes = ChangeSet::try_from(wire).map_err(|error| format!("{error:?}"))?;
    let fixture_operation_reader_15 = receiver
        .db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    receiver
        .db
        .validate_received_row_lineages_with_received_ddl(
            &tenant,
            &changes,
            &lineages,
            &received,
            fixture_operation_reader_15.clone(),
        )
        .map_err(|error| format!("{error:?}"))?;
    let fixture_operation_reader_16 = receiver
        .db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");
    let mut fixture_operation_copy_16 =
        contextdb_engine::vector_read_memory::admission(fixture_operation_reader_16.clone());

    receiver
        .db
        .apply_authenticated_received_changes_with_lineages(
            changes,
            &arrivals,
            SyncAdoption::Continuing,
            None,
            &tenant,
            &lineages,
            Some(&received),
            false,
            &mut fixture_operation_copy_16,
            fixture_operation_reader_16.clone(),
        )
        .map_err(|error| format!("{error:?}"))?;
    Ok(received)
}

/// The durable whole-unit record a receiver keeps for one authenticated
/// schema unit, as it must read for exactly `received`: each marker names
/// the sender, its incarnation, the source LSN and ordinal, in source
/// order, bound to the digest the sender attested.
fn assert_durable_manifest_matches(
    receiver: &JourneyPeer,
    received: &crate::protocol::ReceivedDdlContext,
    when: &str,
) -> DurableReceivedSchemaManifest {
    let manifest = receiver
        .persisted::<DurableReceivedSchemaManifest>(
            &Database::durable_received_schema_manifest_key(received)
                .expect("the unit's manifest key"),
        )
        .unwrap_or_else(|| panic!("the receiver durably recorded the unit {when}"));
    let expected = received
        .entries
        .iter()
        .map(|entry| DurableReceivedSchemaManifestMarker {
            key: Database::durable_received_ddl_marker_key(
                &received.tenant_id,
                &received.source_node_id,
                received.source_incarnation,
                entry.source_ddl_lsn,
                entry.ordinal,
            ),
            digest: entry.digest.clone(),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        manifest.markers, expected,
        "the durable record holds every DDL occurrence's source LSN, ordinal and digest, in \
             exactly the sender's order {when}"
    );
    for marker in &expected {
        let stored = receiver
            .persisted::<DurableReceivedDdlMarker>(&marker.key)
            .unwrap_or_else(|| panic!("each occurrence's own marker is durable {when}"));
        assert_eq!(
            stored.digest, marker.digest,
            "each stored marker keeps the attested digest {when}"
        );
    }
    manifest
}

/// Through authenticated sync, a person creates a
/// table, then a trigger on it, then alters the table, in ONE source
/// transaction. The unit travels exactly as sync sends it -- signed by the
/// author's identity, with source-owned provenance -- and the receiver
/// keeps, durably and across a restart, each statement's source LSN,
/// ordinal and canonical digest in exactly the author's order, beside the
/// altered table and the registered trigger.
#[test]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn a_received_create_trigger_alter_unit_installs_in_authored_order_at_the_door() {
    let root = tempfile::TempDir::new().expect("scratch peer directory");
    let author = JourneyPeer::open(root.path(), "order-author");
    let receiver = JourneyPeer::open(root.path(), "order-receiver");

    let tx = author.db.begin().expect("the author opens one transaction");
    for statement in [
        "CREATE TABLE events (id INTEGER PRIMARY KEY)",
        "CREATE TRIGGER events_audit ON events WHEN INSERT",
        "ALTER TABLE events ADD COLUMN body TEXT",
    ] {
        author
            .db
            .execute_in_tx(tx, statement, &HashMap::new())
            .unwrap_or_else(|error| panic!("the author runs `{statement}`: {error:?}"));
    }
    author.db.commit(tx).expect("the author commits the three");

    let served = served_by(&author, Lsn(0));
    assert_eq!(served.ddl.len(), 3, "one unit of three authored statements");
    let source_lsn = served.ddl_lsn[0];
    assert!(
        served.ddl_lsn.iter().all(|lsn| *lsn == source_lsn),
        "the three statements are one source transaction: {:?}",
        served.ddl_lsn
    );
    assert_eq!(
        served
            .ddl_provenance
            .iter()
            .map(|entry| (entry.source_ddl_lsn, entry.ordinal))
            .collect::<Vec<_>>(),
        vec![(source_lsn, 0), (source_lsn, 1), (source_lsn, 2)],
        "the author attests create, trigger, alter as ordinals 0, 1, 2"
    );
    let authored = served
        .ddl
        .iter()
        .cloned()
        .map(DdlChange::from)
        .collect::<Vec<_>>();
    assert!(
        matches!(
            authored.as_slice(),
            [
                DdlChange::CreateTable { .. },
                DdlChange::CreateTrigger { .. },
                DdlChange::AlterTable { .. }
            ]
        ),
        "the author's order is create, trigger, alter: {authored:?}"
    );

    let pushed = pushed_to(&receiver, &author, served.clone());
    assert!(pushed.result.conflicts.is_empty(), "{:?}", pushed.result);
    let received = pushed.received.expect("the unit carries schema");
    assert_eq!(received.source_node_id, author.node_id());
    let digests = served
        .ddl_provenance
        .iter()
        .map(|entry| entry.digest.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        received
            .entries
            .iter()
            .map(|entry| entry.digest.clone())
            .collect::<Vec<_>>(),
        digests,
        "the authenticated context carries the author's own digests"
    );

    let check = |peer: &JourneyPeer, when: &str| {
        assert_durable_manifest_matches(peer, &received, when);
        let log = peer
            .db
            .changes_since(Lsn(0))
            .expect("read the declared changes");
        let local_lsn = log.ddl_lsn[0];
        assert!(
            log.ddl_lsn.iter().all(|lsn| *lsn == local_lsn),
            "the receiver installed the unit as one commit {when}"
        );
        assert_eq!(
            log.ddl.clone(),
            authored,
            "the receiver's durable DDL log replays create -> trigger -> alter as authored \
                 {when}"
        );
        for (ordinal, entry) in served.ddl_provenance.iter().enumerate() {
            let ordinal = ordinal as u32;
            let fixture_operation_reader_1 = peer
                .db
                .declared_reader_memory()
                .expect("admit the actual fixture reader");

            assert!(
                peer.db
                    .with_received_ddl_arrival(
                        (local_lsn, ordinal),
                        |arrival| Ok(arrival.is_some()),
                        fixture_operation_reader_1.clone()
                    )
                    .expect("read the durable received occurrence"),
                "occurrence {ordinal} is recorded as received {when}"
            );
            let generation = peer
                .db
                .recorded_ddl_generation_sidecar(local_lsn, ordinal)
                .expect("read the generation sidecar")
                .unwrap_or_else(|| panic!("occurrence {ordinal} keeps its generation {when}"));
            assert_eq!(
                (generation.table, generation.table_generation),
                (entry.table.clone(), entry.table_generation),
                "occurrence {ordinal} keeps the author's table generation {when}"
            );
        }
        let meta = peer
            .db
            .table_meta("events")
            .unwrap_or_else(|| panic!("the table is live {when}"));
        assert_eq!(
            meta.columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "body"],
            "the table has the alter's shape {when}"
        );
        assert!(
            peer.db
                .trigger
                .declarations
                .lock()
                .contains_key("events_audit"),
            "the trigger declared between the create and the alter is registered {when}"
        );
    };
    check(&receiver, "after install");
    let receiver = receiver.reopen();
    check(&receiver, "after restart");
    receiver.db.close().expect("close the receiver");
    author.db.close().expect("close the author");
}

/// Reduce an authored/received DDL vector to (kind, name) pairs, so an
/// author-order proof does not depend on the parser's exact column-type
/// spelling for a freshly created table.
fn ddl_kinds_and_names(ddl: &[DdlChange]) -> Vec<(&'static str, String)> {
    ddl.iter()
        .map(|change| match change {
            DdlChange::CreateTable { name, .. } => ("CreateTable", name.clone()),
            DdlChange::CreateTrigger { name, .. } => ("CreateTrigger", name.clone()),
            DdlChange::DropTable { name } => ("DropTable", name.clone()),
            DdlChange::DropTrigger { name } => ("DropTrigger", name.clone()),
            other => ("Other", format!("{other:?}")),
        })
        .collect()
}

/// A signed unit carrying a `ReceivedDdlContext` and a drop must replay,
/// never take `received_schema_unit_is_projection_noop`'s "changes
/// nothing" exit: the `carries_a_drop` clause applies to
/// this authenticated door exactly as it does to the unattested one
/// (`t3_sync_trigger_table_create_drop_history_replays_chronologically`
/// guards the unattested case). A local author cannot put this exact
/// sequence inside one `BEGIN`/`COMMIT` -- `DROP TABLE` is refused inside
/// a transactional local schema stage -- so the unit has the shape a
/// signed delivery carries: a single source LSN whose declarations a
/// receiver installs whole. The same unit installs identically on two
/// independently receiving peers.
#[test]
fn a_signed_unit_carrying_a_drop_replays_instead_of_taking_the_noop_exit() {
    let ddl = vec![
        DdlChange::CreateTable {
            name: "x".to_string(),
            columns: vec![("id".to_string(), "UUID PRIMARY KEY".to_string())],
            constraints: Vec::new(),
            foreign_keys: Vec::new(),
            composite_foreign_keys: Vec::new(),
            composite_unique: Vec::new(),
        },
        DdlChange::CreateTrigger {
            name: "tx".to_string(),
            table: "x".to_string(),
            on_events: vec!["INSERT".to_string()],
        },
        DdlChange::DropTable {
            name: "x".to_string(),
        },
        DdlChange::DropTrigger {
            name: "tx".to_string(),
        },
    ];
    let changes = ChangeSet {
        ddl: ddl.clone(),
        ddl_lsn: vec![Lsn(41), Lsn(41), Lsn(41), Lsn(41)],
        ..ChangeSet::default()
    };
    let received = crate::protocol::ReceivedDdlContext {
        tenant_id: TenantId::from("tenant-a"),
        source_node_id: "authenticated-source".to_string(),
        source_incarnation: Incarnation(7),
        entries: vec![
            crate::protocol::ReceivedDdlEntry {
                source_ddl_lsn: Lsn(41),
                ordinal: 0,
                table: Some("x".to_string()),
                table_generation: Some(1),
                digest: vec![1],
                author: None,
            },
            crate::protocol::ReceivedDdlEntry {
                source_ddl_lsn: Lsn(41),
                ordinal: 1,
                table: Some("x".to_string()),
                table_generation: Some(1),
                digest: vec![2],
                author: None,
            },
            crate::protocol::ReceivedDdlEntry {
                source_ddl_lsn: Lsn(41),
                ordinal: 2,
                table: Some("x".to_string()),
                table_generation: Some(1),
                digest: vec![3],
                author: None,
            },
            crate::protocol::ReceivedDdlEntry {
                source_ddl_lsn: Lsn(41),
                ordinal: 3,
                table: None,
                table_generation: None,
                digest: vec![4],
                author: None,
            },
        ],
    };

    let check = |peer: &Database, when: &str| {
        let received_schema_read_memory_1 = peer
            .declared_reader_memory()
            .expect("admit the received-schema operation reader");
        let mut received_schema_copy_owner_1 =
            contextdb_engine::vector_read_memory::admission(received_schema_read_memory_1.clone());

        peer.commit_received_schema_stage_for_test(
            &changes,
            &received,
            &[],
            &HashMap::new(),
            false,
            &mut received_schema_copy_owner_1,
            received_schema_read_memory_1,
        )
        .unwrap_or_else(|error| panic!("{when}: the signed drop unit installs: {error:?}"));
        let log = peer
            .changes_since(Lsn(0))
            .expect("read the declared changes");
        assert_eq!(
            ddl_kinds_and_names(&log.ddl),
            ddl_kinds_and_names(&ddl),
            "{when}: a signed unit carrying a drop must replay in author order, never take \
                 the changes-nothing exit"
        );
        let local_lsn = log.ddl_lsn[0];
        assert!(
            log.ddl_lsn.iter().all(|lsn| *lsn == local_lsn),
            "{when}: the drop stays one receiver commit boundary: {:?}",
            log.ddl_lsn
        );
        assert!(
            peer.table_meta("x").is_none(),
            "{when}: the dropped table is gone"
        );
        assert!(
            peer.list_triggers()
                .iter()
                .all(|trigger| trigger.name != "tx"),
            "{when}: the trigger declared and dropped in the same unit is gone"
        );
    };
    check(&Database::open_memory(), "at the hub");
    check(
        &Database::open_memory(),
        "at a second, independently receiving edge",
    );
}

// ---- A forwarding hub cannot rewrite what the author signed -----------
//
// Authenticated received DDL is immutable after
// transport validation; its signed provenance -- source LSN, ordinal,
// generation and digest -- attests to the schema the author actually sent,
// and every receiver applies the author's exact vector in the author's
// exact order. Each proof below runs a real author -> hub -> edge journey
// between file-backed peers that each hold their own key. The hub takes
// the author's signed unit honestly, then serves it onward with one change
// it makes on its own authority: the unsigned per-hop fields (its own
// delivery ordinal and transport digest) are re-derived so every check
// that does not consult the author's signature still agrees with itself.
// The edge must refuse the whole unit at both authenticated doors (pull
// and receipted push) before any schema, receipt or progress change, keep
// refusing nothing into its store across a restart, and then still take
// the untouched unit in the author's order.

const FORWARDED_UNIT: [&str; 3] = [
    "CREATE TABLE alpha (id UUID PRIMARY KEY)",
    "CREATE TABLE beta (id UUID PRIMARY KEY)",
    "CREATE TABLE gamma (id UUID PRIMARY KEY)",
];
const FORWARDED_TABLES: [&str; 3] = ["alpha", "beta", "gamma"];

/// The author writes three independent tables in one source transaction
/// (independent, so a reordered or shortened unit still installs cleanly
/// and only the author's signature can tell), pushes it to the hub, and
/// the hub serves it onward exactly as it would to any edge.
fn forwarded_unit_journey(
    root: &std::path::Path,
) -> (
    JourneyPeer,
    JourneyPeer,
    JourneyPeer,
    crate::protocol::WireChangeSet,
) {
    let author = JourneyPeer::open(root, "signed-unit-author");
    let hub = JourneyPeer::open(root, "signed-unit-hub");
    let edge = JourneyPeer::open(root, "signed-unit-edge");
    let tx = author.db.begin().expect("the author opens one transaction");
    for statement in FORWARDED_UNIT {
        author
            .db
            .execute_in_tx(tx, statement, &HashMap::new())
            .unwrap_or_else(|error| panic!("the author runs `{statement}`: {error:?}"));
    }
    author.db.commit(tx).expect("the author commits its unit");
    let from_author = served_by(&author, Lsn(0));
    assert_eq!(from_author.ddl.len(), 3, "one unit of three statements");
    let pushed = pushed_to(&hub, &author, from_author);
    assert!(pushed.result.conflicts.is_empty(), "{:?}", pushed.result);
    let from_hub = served_by(&hub, Lsn(0));
    assert_eq!(from_hub.ddl.len(), 3, "the hub serves the whole unit");
    for provenance in &from_hub.ddl_provenance {
        let named = provenance
            .author
            .as_ref()
            .expect("the hub serves the author's signed identity");
        assert_eq!(named.node_id, author.node_id(), "under the author's name");
        assert_eq!(
            named.table_generation, provenance.table_generation,
            "an honest hub serves the generation the author signed"
        );
    }
    (author, hub, edge, from_hub)
}

/// Re-derive the per-hop fields a forwarding peer owns -- its own delivery
/// ordinal and the unkeyed transport digest -- over whatever it now
/// serves, so the tampered unit is self-consistent everywhere except
/// against the author's signature.
fn reseal_delivery_positions(wire: &mut crate::protocol::WireChangeSet) {
    let mut next = HashMap::<Lsn, u32>::new();
    for ((ddl, lsn), provenance) in wire
        .ddl
        .iter()
        .zip(&wire.ddl_lsn)
        .zip(wire.ddl_provenance.iter_mut())
    {
        let ordinal = next.entry(*lsn).or_default();
        provenance.ordinal = *ordinal;
        *ordinal += 1;
        provenance.digest = crate::protocol::canonical_ddl_provenance_digest(
            ddl,
            provenance.source_ddl_lsn,
            provenance.ordinal,
            provenance.table.as_deref(),
            provenance.table_generation,
        )
        .expect("the hub's own transport digest");
    }
}

/// Everything a refused delivery must leave untouched on the receiver:
/// its commit position, its schema and DDL log, every durable config
/// value (received markers, arrivals, whole-unit records, generation
/// sidecars, receipts, per-request outcomes, applied-push and sync
/// watermarks), and the progress it reports for the forwarding hub.
#[derive(Debug, PartialEq)]
struct ReceiverImage {
    lsn: Lsn,
    ddl: Vec<(Lsn, DdlChange)>,
    tables: Vec<(String, bool)>,
    received_arrivals: usize,
    watermarks: (Lsn, Lsn),
    applied_from_hub: Option<Lsn>,
    config: Vec<(String, Vec<u8>)>,
}

fn receiver_image(receiver: &JourneyPeer, hub: &JourneyPeer) -> ReceiverImage {
    let tenant = TenantId::from(JOURNEY_TENANT);
    let log = receiver
        .db
        .changes_since(Lsn(0))
        .expect("read the declared changes");
    ReceiverImage {
        lsn: receiver.db.current_lsn(),
        ddl: log.ddl_lsn.into_iter().zip(log.ddl).collect(),
        tables: FORWARDED_TABLES
            .iter()
            .map(|table| (table.to_string(), receiver.db.table_meta(table).is_some()))
            .collect(),
        received_arrivals: receiver.db.received_ddl_arrivals.read().len(),
        watermarks: receiver
            .db
            .persisted_sync_watermarks(&tenant)
            .expect("read the receiver's sync progress"),
        applied_from_hub: receiver
            .db
            .persisted_sync_applied_push_watermark_for_node(&tenant, &hub.node_id())
            .expect("read the receiver's applied-push progress for the hub"),
        config: receiver
            .db
            .persistence
            .as_ref()
            .expect("a file-backed receiver")
            .dump_config_raw()
            .expect("read every durable config value"),
    }
}

/// The edge refuses `tampered` whole at both authenticated doors, before
/// and after a restart nothing of it is stored, and the untouched unit the
/// hub really holds still installs afterwards in the author's order with
/// the generations the author signed.
fn assert_edge_refuses_the_whole_unit(
    edge: JourneyPeer,
    hub: &JourneyPeer,
    honest: crate::protocol::WireChangeSet,
    tampered: crate::protocol::WireChangeSet,
    what: &str,
    refusal: &str,
) {
    let before = receiver_image(&edge, hub);
    assert!(
        before.tables.iter().all(|(_, present)| !present),
        "the edge starts without the unit"
    );
    let pulled = try_pulled_from(&edge, hub, tampered.clone());
    assert!(
        pulled.is_err(),
        "an edge pulling from a hub must refuse a unit whose {what} -- the author's \
             signature does not cover what is being installed; it installed instead"
    );
    let error = pulled.err().unwrap_or_default();
    assert!(
        error.contains(refusal),
        "the pull refusal names the signed-author check it failed: {error}"
    );
    assert_eq!(
        receiver_image(&edge, hub),
        before,
        "a refused pull leaves no schema, receipt or progress change ({what})"
    );
    let pushed = try_pushed_to(&edge, hub, tampered);
    assert!(
        pushed.is_err(),
        "the receipted push door must refuse the same unit whose {what}; it installed"
    );
    let error = pushed.err().unwrap_or_default();
    assert!(
        error.contains(refusal),
        "the push refusal names the signed-author check it failed: {error}"
    );
    assert_eq!(
        receiver_image(&edge, hub),
        before,
        "a refused push leaves no schema, receipt or progress change ({what})"
    );
    let edge = edge.reopen();
    assert_eq!(
        receiver_image(&edge, hub),
        before,
        "after a restart the refused unit has left nothing behind ({what})"
    );

    // The refusal is not sticky: the unit the author really signed, as
    // the hub really holds it, installs whole and in the author's order.
    pulled_from(&edge, hub, honest.clone());
    let log = edge
        .db
        .changes_since(Lsn(0))
        .expect("read the declared changes");
    assert_eq!(
        log.ddl
            .iter()
            .map(|ddl| match ddl {
                DdlChange::CreateTable { name, .. } => name.clone(),
                other => panic!("only the author's creates arrive: {other:?}"),
            })
            .collect::<Vec<_>>(),
        FORWARDED_TABLES.map(str::to_string).to_vec(),
        "the untouched unit installs in the author's order"
    );
    let local_lsn = log.ddl_lsn[0];
    for (ordinal, provenance) in honest.ddl_provenance.iter().enumerate() {
        let generation = edge
            .db
            .recorded_ddl_generation_sidecar(local_lsn, ordinal as u32)
            .expect("read the generation sidecar")
            .expect("each installed statement keeps its generation");
        assert_eq!(
            generation.table_generation,
            provenance
                .author
                .as_ref()
                .expect("the honest unit names its author")
                .table_generation,
            "each statement installs against the generation its author signed"
        );
    }
    edge.db.close().expect("close the edge");
}

#[test]
fn an_edge_refuses_a_forwarded_statement_whose_generation_is_not_the_one_its_author_signed() {
    let root = tempfile::TempDir::new().expect("scratch peer directory");
    let (author, hub, edge, from_hub) = forwarded_unit_journey(root.path());
    let mut tampered = from_hub.clone();
    let signed = tampered.ddl_provenance[1]
        .author
        .as_ref()
        .expect("the author's signed identity")
        .table_generation
        .expect("a table statement binds a generation");
    tampered.ddl_provenance[1].table_generation = Some(signed + 7);
    reseal_delivery_positions(&mut tampered);
    assert_edge_refuses_the_whole_unit(
        edge,
        &hub,
        from_hub,
        tampered,
        "delivered table generation differs from the one its author signed",
        "table generation its author did not sign",
    );
    hub.db.close().expect("close the hub");
    author.db.close().expect("close the author");
}

#[test]
fn an_edge_refuses_a_forwarded_unit_whose_signed_statements_were_swapped() {
    let root = tempfile::TempDir::new().expect("scratch peer directory");
    let (author, hub, edge, from_hub) = forwarded_unit_journey(root.path());
    let mut tampered = from_hub.clone();
    tampered.ddl.swap(0, 1);
    tampered.ddl_provenance.swap(0, 1);
    reseal_delivery_positions(&mut tampered);
    assert_edge_refuses_the_whole_unit(
        edge,
        &hub,
        from_hub,
        tampered,
        "first two signed statements were swapped by the forwarding hub",
        "in the author's signed order",
    );
    hub.db.close().expect("close the hub");
    author.db.close().expect("close the author");
}

#[test]
fn an_edge_refuses_a_forwarded_unit_missing_one_of_its_authors_signed_statements() {
    let root = tempfile::TempDir::new().expect("scratch peer directory");
    let (author, hub, edge, from_hub) = forwarded_unit_journey(root.path());
    // Dropping the author's LAST statement leaves no gap in the ordinals
    // that remain, so only a signed account of the whole unit can see it.
    let mut tampered = from_hub.clone();
    tampered.ddl.pop();
    tampered.ddl_lsn.pop();
    tampered.ddl_provenance.pop();
    reseal_delivery_positions(&mut tampered);
    // Restating the author's statement count to match what is left
    // breaks the author's own signature instead.
    let mut recounted = tampered.clone();
    for provenance in &mut recounted.ddl_provenance {
        provenance
            .author
            .as_mut()
            .expect("the author's signed identity")
            .unit_statements = 2;
    }
    let before = receiver_image(&edge, &hub);
    let error = try_pulled_from(&edge, &hub, recounted)
        .expect_err("a unit whose signed statement count was rewritten is refused");
    assert!(
        error.contains("does not verify for the author it names"),
        "the refusal names the author's signature: {error}"
    );
    assert_eq!(receiver_image(&edge, &hub), before, "and changes nothing");
    assert_edge_refuses_the_whole_unit(
        edge,
        &hub,
        from_hub,
        tampered,
        "author's last signed statement was removed by the forwarding hub",
        "only part of its author's source transaction",
    );
    hub.db.close().expect("close the hub");
    author.db.close().expect("close the author");
}

#[test]
fn received_schema_source_order_preserves_mixed_lsn_marker_identities_and_digests() {
    // This raw codec fixture declares its own writer operation; it has no
    // database request whose ambient owner it may borrow.
    let _fixture_decode_scope = crate::read_image_memory::ImageMemory::enter_writer_operation(
        crate::memory_accounting::AccountantHandle::standalone(None, true).unwrap(),
    )
    .unwrap();

    let ddl = vec![create_table(), create_trigger()];
    let received = crate::protocol::ReceivedDdlContext {
        tenant_id: TenantId::from("tenant-a"),
        source_node_id: "node-a".to_string(),
        source_incarnation: Incarnation(7),
        entries: vec![
            crate::protocol::ReceivedDdlEntry {
                source_ddl_lsn: Lsn(5),
                ordinal: 7,
                table: Some("events".to_string()),
                table_generation: Some(3),
                digest: vec![0xaa],
                author: None,
            },
            crate::protocol::ReceivedDdlEntry {
                source_ddl_lsn: Lsn(99),
                ordinal: 2,
                table: Some("events".to_string()),
                table_generation: Some(3),
                digest: vec![0xbb, 0xcc],
                author: None,
            },
        ],
    };
    let (_, payload) =
        plan_received_schema_source_order(Lsn(9), &ddl, &[Lsn(5), Lsn(99)], Some(&received))
            .expect("mixed source provenance plan");

    assert_eq!(
        payload.marker_values[0].0,
        "sync_ddl_apply.v1.6e6f64652d61.inc.00000000000000000000000000000007.lsn.00000000000000000005.000007:tenant-a"
    );
    assert_eq!(
        payload.marker_values[1].0,
        "sync_ddl_apply.v1.6e6f64652d61.inc.00000000000000000000000000000007.lsn.00000000000000000099.000002:tenant-a"
    );
    let first: DurableReceivedDdlMarker =
        RedbPersistence::decode_config_value(&payload.marker_values[0].1).unwrap();
    let second: DurableReceivedDdlMarker =
        RedbPersistence::decode_config_value(&payload.marker_values[1].1).unwrap();
    assert_eq!(first.digest, vec![0xaa]);
    assert_eq!(second.digest, vec![0xbb, 0xcc]);
    assert_eq!(
        payload
            .received_arrival_values
            .iter()
            .map(|(key, _)| key.as_str())
            .collect::<Vec<_>>(),
        vec![
            "sync_ddl_received_arrival.v1.00000000000000000009.000000",
            "sync_ddl_received_arrival.v1.00000000000000000009.000001",
        ]
    );
    let local_first: DurableReceivedDdlArrival =
        RedbPersistence::decode_config_value(&payload.received_arrival_values[0].1).unwrap();
    let local_second: DurableReceivedDdlArrival =
        RedbPersistence::decode_config_value(&payload.received_arrival_values[1].1).unwrap();
    assert_eq!(
        (local_first.local_lsn, local_first.persisted_ordinal),
        (Lsn(9), 0)
    );
    assert_eq!(
        (local_second.local_lsn, local_second.persisted_ordinal),
        (Lsn(9), 1)
    );
    assert_ne!(local_first.digest, first.digest);
    assert_ne!(local_second.digest, second.digest);
}

fn received_arrival(
    db: &Database,
    ddl: &DdlChange,
    lsn: Lsn,
    ordinal: u32,
) -> DurableReceivedDdlArrival {
    let generation = DurableDdlGenerationSidecar {
        table: Database::ddl_affected_table(ddl).map(str::to_string),
        table_generation: Some(1),
    };
    db.in_memory_ddl_generations.lock().insert(
        (lsn, ordinal),
        (generation.table.clone(), generation.table_generation),
    );
    if let (Some(table), Some(table_generation)) = (&generation.table, generation.table_generation)
    {
        db.in_memory_table_generations
            .lock()
            .insert(table.clone(), table_generation);
    }
    db.ddl_log.write().push((lsn, ddl.clone()));
    DurableReceivedDdlArrival {
        local_lsn: lsn,
        persisted_ordinal: ordinal,
        digest: Database::canonical_local_ddl_arrival_digest(ddl, lsn, ordinal, &generation)
            .unwrap(),
        author: None,
    }
}

#[test]
fn received_ddl_filter_matches_duplicate_occurrences_and_destination_frontier() {
    let db = Database::open_memory();
    let duplicate = create_trigger();
    let later_received = create_trigger();
    let local = alter_table();
    let changes = ChangeSet {
        ddl: vec![
            duplicate.clone(),
            duplicate.clone(),
            later_received.clone(),
            local.clone(),
        ],
        ddl_lsn: vec![Lsn(5), Lsn(5), Lsn(7), Lsn(9)],
        ..ChangeSet::default()
    };
    let source = DdlProvenanceSource::historical(&changes);
    let first = received_arrival(&db, &duplicate, Lsn(5), 0);
    let second = received_arrival(&db, &duplicate, Lsn(5), 1);
    let later = received_arrival(&db, &later_received, Lsn(7), 0);
    let _local = received_arrival(&db, &local, Lsn(9), 0);
    db.received_ddl_arrivals.write().extend([
        ((Lsn(5), 0), first),
        ((Lsn(5), 1), second),
        ((Lsn(7), 0), later),
    ]);
    let fixture_operation_reader_17 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    let ordinary = db
        .filter_outbound_received_ddl(
            changes.clone(),
            &source,
            None,
            fixture_operation_reader_17.clone(),
        )
        .unwrap();
    assert_eq!(ordinary.ddl, vec![local.clone()]);
    assert_eq!(ordinary.ddl_lsn, vec![Lsn(9)]);

    let held_changes = ChangeSet {
        ddl: vec![duplicate.clone(), duplicate.clone(), later_received],
        ddl_lsn: vec![Lsn(5), Lsn(5), Lsn(7)],
        ..ChangeSet::default()
    };
    let held_source = DdlProvenanceSource::historical(&held_changes);
    let fixture_operation_reader_18 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    let destination = db
        .filter_outbound_received_ddl(
            held_changes,
            &held_source,
            Some(Lsn(5)),
            fixture_operation_reader_18.clone(),
        )
        .unwrap();
    assert_eq!(destination.ddl, vec![duplicate.clone(), duplicate]);
    assert_eq!(destination.ddl_lsn, vec![Lsn(5), Lsn(5)]);
    let fixture_operation_reader_19 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    let error = db
        .filter_outbound_received_ddl(
            changes,
            &source,
            Some(Lsn(5)),
            fixture_operation_reader_19.clone(),
        )
        .expect_err("post-move schema without positive origin evidence must fail closed");
    assert!(
        error
            .to_string()
            .contains("schema published after the move"),
        "the destination frontier diagnostic must name the unsafe schema boundary: {error}"
    );
}

#[test]
fn received_ddl_filter_fails_closed_for_partial_or_mismatched_arrivals() {
    let db = Database::open_memory();
    let duplicate = create_trigger();
    let changes = ChangeSet {
        ddl: vec![duplicate.clone(), duplicate.clone()],
        ddl_lsn: vec![Lsn(5), Lsn(5)],
        ..ChangeSet::default()
    };
    let source = DdlProvenanceSource::historical(&changes);
    let first = received_arrival(&db, &duplicate, Lsn(5), 0);
    let _second = received_arrival(&db, &duplicate, Lsn(5), 1);
    db.received_ddl_arrivals.write().insert((Lsn(5), 0), first);
    let fixture_operation_reader_20 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    assert!(
        db.filter_outbound_received_ddl(
            changes.clone(),
            &source,
            None,
            fixture_operation_reader_20.clone()
        )
        .is_err()
    );

    let mut mismatched = received_arrival(&db, &duplicate, Lsn(5), 1);
    mismatched.digest = vec![0];
    db.received_ddl_arrivals
        .write()
        .insert((Lsn(5), 1), mismatched);
    let fixture_operation_reader_21 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    assert!(
        db.filter_outbound_received_ddl(
            changes,
            &source,
            None,
            fixture_operation_reader_21.clone()
        )
        .is_err()
    );
}

#[test]
fn received_ddl_marker_binds_the_stored_occurrence_not_the_outbound_spelling() {
    let db = Database::open_memory();
    let raw = create_table();
    let mut outbound = raw.clone();
    let DdlChange::CreateTable {
        composite_unique, ..
    } = &mut outbound
    else {
        unreachable!("create_table fixture must remain table DDL");
    };
    *composite_unique = vec![vec!["id".to_string()]];
    let changes = ChangeSet {
        ddl: vec![outbound],
        ddl_lsn: vec![Lsn(5)],
        ..ChangeSet::default()
    };
    let source = DdlProvenanceSource::historical(&changes);
    let arrival = received_arrival(&db, &raw, Lsn(5), 0);
    db.received_ddl_arrivals
        .write()
        .insert((Lsn(5), 0), arrival);
    let fixture_operation_reader_22 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    assert!(
        db.filter_outbound_received_ddl(
            changes,
            &source,
            None,
            fixture_operation_reader_22.clone()
        )
        .unwrap()
        .ddl
        .is_empty()
    );
}

#[test]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn filtered_synthetic_schema_signs_only_its_remaining_authored_order() {
    let root = tempfile::TempDir::new().expect("scratch signing identity");
    let identity = crate::identity::FabricIdentity::load_or_generate(&root.path().join("node.key"))
        .expect("create the signing identity");
    let db = Database::open_memory();
    for statement in [
        "CREATE TABLE events (id INTEGER PRIMARY KEY)",
        "CREATE TABLE notes (id INTEGER PRIMARY KEY)",
    ] {
        db.execute(statement, &HashMap::new())
            .expect("declare application tables");
    }
    let named_table = |name: &str| DdlChange::CreateTable {
        name: name.to_string(),
        columns: vec![("id".to_string(), "INTEGER PRIMARY KEY".to_string())],
        constraints: Vec::new(),
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    };
    let notes = named_table("notes");
    let contact = named_table("work_node_contacts");
    let original = ChangeSet {
        ddl: vec![create_table(), contact, notes.clone()],
        ddl_lsn: vec![Lsn(100); 3],
        ..ChangeSet::default()
    };
    let source = DdlProvenanceSource::synthetic_snapshot(&original);
    let filtered = crate::sync_server::without_hub_local_bookkeeping(original);
    assert_eq!(filtered.ddl, vec![create_table(), notes]);
    let tenant = TenantId::from("filtered-synthetic-schema");
    let fixture_operation_reader_23 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    let provenance = db
        .outbound_ddl_provenance(
            &filtered,
            &source,
            &tenant,
            &identity.node_id(),
            Incarnation(7),
            &|bytes| Ok(identity.sign_lineage(bytes)),
            fixture_operation_reader_23.clone(),
        )
        .expect("sign the filtered application schema");
    assert_eq!(
        provenance
            .iter()
            .map(|entry| (
                entry.ordinal,
                entry.author.as_ref().unwrap().unit_statements
            ))
            .collect::<Vec<_>>(),
        vec![(0, 2), (1, 2)],
        "the excluded system statement occupies no signed position"
    );
    let wire = crate::protocol::wire_changeset_with_arrivals_lineages_and_ddl_provenance(
        filtered,
        &HashMap::new(),
        &HashMap::new(),
        provenance,
    );
    crate::protocol::validate_wire_ddl_provenance(&wire)
        .expect("the exact remaining authored unit validates in order");
    for (ddl, entry) in wire.ddl.iter().zip(&wire.ddl_provenance) {
        crate::protocol::verify_ddl_author_attestation(
            &tenant,
            ddl,
            entry.author.as_ref().expect("locally authored statement"),
        )
        .expect("each remaining statement verifies under the local author");
    }
}

fn accepted_delete_fixture() -> (tempfile::TempDir, Database, NaturalKey) {
    let temp = tempfile::TempDir::new().unwrap();
    let db = Database::open(temp.path().join("accepted-delete.redb")).unwrap();
    db.execute(
        "CREATE TABLE events (id INTEGER, scope TEXT, PRIMARY KEY (id, scope))",
        &HashMap::new(),
    )
    .unwrap();
    let key = NaturalKey::from_pairs(vec![
        ("id".to_string(), Value::Int64(1)),
        ("scope".to_string(), Value::Text("old".to_string())),
    ])
    .unwrap();
    (temp, db, key)
}

fn accepted_delete_record(db: &Database, key: &NaturalKey, root: &str) {
    let fixture_operation_reader_3 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    let generation = db
        .durable_lineage_table_generation("events", fixture_operation_reader_3.clone())
        .unwrap();
    let record = DurableLineageRecord {
        table: "events".to_string(),
        natural_key: key.clone(),
        table_generation: generation,
        local_row_id: Some(RowId(1)),
        locally_created: false,
        lineage_root: root.to_string(),
        lineage_attestation: vec![1],
        author_node_id: Some("origin".to_string()),
        author_database_incarnation: Some(Incarnation(7).to_hex()),
        author_local_mutation_position: 11,
        delete_lsn: 4,
        delete_obligation: DurableDeleteObligation::Accepted,
        accepted_hub_lsn: Some(4),
        bound_hub_node_id: Some("hub".to_string()),
        purge_frontier: None,
    };
    db.persistence
        .as_ref()
        .unwrap()
        .flush_config_value(
            &Database::durable_lineage_config_key("events", key, generation),
            &record,
        )
        .unwrap();
}

fn accepted_delete_lineage(
    key: &NaturalKey,
    lsn: Lsn,
    root: &str,
) -> Vec<(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)> {
    vec![(
        ("events".to_string()),
        key.clone(),
        lsn,
        crate::protocol::WireRowLineage {
            author_node_id: "origin".to_string(),
            author_database_incarnation: Incarnation(7),
            author_local_mutation_position: Lsn(11),
            table_generation: 1,
            lineage_root: root.to_string(),
            attestation: vec![1],
        },
    )]
}

fn accepted_delete_row(key: NaturalKey, lsn: Lsn, deleted: bool) -> RowChange {
    RowChange {
        table: "events".to_string(),
        natural_key: key,
        values: HashMap::from([
            ("id".to_string(), Value::Int64(1)),
            ("scope".to_string(), Value::Text("old".to_string())),
        ]),
        deleted,
        lsn,
        created_at: None,
    }
}

fn projected_vector_ddl() -> DdlChange {
    DdlChange::AlterTable {
        name: "events".to_string(),
        columns: vec![
            ("id".to_string(), "INTEGER".to_string()),
            ("scope".to_string(), "TEXT".to_string()),
            ("embedding".to_string(), "VECTOR(2)".to_string()),
        ],
        constraints: vec!["PRIMARY KEY (id, scope)".to_string()],
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    }
}

#[test]
fn accepted_delete_ordinary_suppression_keeps_delete_tombstone_and_unrelated_owner() {
    let (_temp, db, key) = accepted_delete_fixture();
    accepted_delete_record(&db, &key, "root-old");
    let old = accepted_delete_row(key.clone(), Lsn(5), false);
    let unrelated_key = NaturalKey::from_pairs(vec![
        ("id".to_string(), Value::Int64(2)),
        ("scope".to_string(), Value::Text("other".to_string())),
    ])
    .unwrap();
    let unrelated = RowChange {
        table: "events".to_string(),
        natural_key: unrelated_key,
        values: HashMap::from([
            ("id".to_string(), Value::Int64(2)),
            ("scope".to_string(), Value::Text("other".to_string())),
        ]),
        deleted: false,
        lsn: Lsn(5),
        created_at: None,
    };
    let delete = accepted_delete_row(key.clone(), Lsn(6), true);
    let index = VectorIndexRef::new("events", "embedding");
    let changes = ChangeSet {
        rows: vec![old.clone(), unrelated.clone(), delete.clone()],
        vectors: vec![
            VectorChange {
                index: index.clone(),
                row_id: RowId(10),
                vector: vec![1.0, 2.0],
                lsn: Lsn(5),
            },
            VectorChange {
                index: index.clone(),
                row_id: RowId(12),
                vector: vec![3.0, 4.0],
                lsn: Lsn(5),
            },
            VectorChange {
                index,
                row_id: RowId(10),
                vector: Vec::new(),
                lsn: Lsn(6),
            },
        ],
        edges: vec![EdgeChange {
            source: uuid::Uuid::from_u128(1),
            target: uuid::Uuid::from_u128(2),
            edge_type: "related".to_string(),
            properties: HashMap::new(),
            lsn: Lsn(5),
        }],
        ddl: vec![projected_vector_ddl()],
        ddl_lsn: vec![Lsn(5)],
    };
    let expected_ddl = changes.ddl.clone();
    let expected_vectors = vec![changes.vectors[1].clone(), changes.vectors[2].clone()];
    let expected_edges = changes.edges.clone();
    let fixture_operation_reader_24 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    let result = db
        .suppress_accepted_lineage_replays(
            changes,
            &accepted_delete_lineage(&key, Lsn(5), "root-old"),
            false,
            fixture_operation_reader_24.clone(),
        )
        .unwrap();
    assert!(result.suppressed_live_replay);
    assert_eq!(result.changes.rows, vec![unrelated, delete]);
    assert_eq!(result.changes.vectors, expected_vectors);
    assert_eq!(result.changes.edges, expected_edges);
    assert_eq!(result.changes.ddl, expected_ddl);
    assert_eq!(result.changes.ddl_lsn, vec![Lsn(5)]);
}

#[test]
fn accepted_delete_ordinary_suppression_requires_absent_full_key_and_exact_root() {
    let (_temp, db, key) = accepted_delete_fixture();
    accepted_delete_record(&db, &key, "root-old");
    let old = accepted_delete_row(key.clone(), Lsn(5), false);
    let old_lineage = accepted_delete_lineage(&key, Lsn(5), "root-old");
    let fixture_operation_reader_25 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    let suppressed = db
        .suppress_accepted_lineage_replays(
            ChangeSet {
                rows: vec![old.clone()],
                ..ChangeSet::default()
            },
            &old_lineage,
            false,
            fixture_operation_reader_25.clone(),
        )
        .unwrap();
    assert!(suppressed.suppressed_live_replay && suppressed.changes.is_empty());
    db.execute(
        "INSERT INTO events (id, scope) VALUES (1, 'other')",
        &HashMap::new(),
    )
    .unwrap();
    let fixture_operation_reader_26 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    assert!(
        db.suppress_accepted_lineage_replays(
            ChangeSet {
                rows: vec![old.clone()],
                ..ChangeSet::default()
            },
            &old_lineage,
            false,
            fixture_operation_reader_26.clone()
        )
        .unwrap()
        .suppressed_live_replay
    );
    db.execute(
        "INSERT INTO events (id, scope) VALUES (1, 'old')",
        &HashMap::new(),
    )
    .unwrap();
    let fixture_operation_reader_27 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    assert!(
        db.suppress_accepted_lineage_replays(
            ChangeSet {
                rows: vec![old.clone()],
                ..ChangeSet::default()
            },
            &old_lineage,
            false,
            fixture_operation_reader_27.clone()
        )
        .is_err()
    );
    let different = accepted_delete_lineage(&key, Lsn(5), "root-fresh");
    let fixture_operation_reader_28 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    let kept = db
        .suppress_accepted_lineage_replays(
            ChangeSet {
                rows: vec![old.clone()],
                ..ChangeSet::default()
            },
            &different,
            false,
            fixture_operation_reader_28.clone(),
        )
        .unwrap();
    assert_eq!(kept.changes.rows, vec![old.clone()]);
    assert!(!kept.suppressed_live_replay);
    let fixture_operation_reader_29 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    assert!(
        db.suppress_accepted_lineage_replays(
            ChangeSet {
                rows: vec![old],
                ..ChangeSet::default()
            },
            &old_lineage,
            true,
            fixture_operation_reader_29.clone()
        )
        .is_err()
    );
}

fn signed_lineage(
    db: &Database,
    tenant: &TenantId,
    key: &NaturalKey,
    position: u64,
    identity: &crate::identity::FabricIdentity,
) -> crate::protocol::WireRowLineage {
    let fixture_operation_reader_4 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    let generation = db
        .durable_lineage_table_generation("events", fixture_operation_reader_4.clone())
        .unwrap();
    let author_node_id = identity.node_id();
    let incarnation = Incarnation(7);
    let lineage_root = format!(
        "author:{author_node_id}:{}:{position}",
        incarnation.to_hex()
    );
    let bytes = Database::lineage_attestation_bytes(
        tenant,
        "events",
        key,
        generation,
        &author_node_id,
        incarnation,
        Lsn(position),
        &lineage_root,
    )
    .unwrap();
    crate::protocol::WireRowLineage {
        author_node_id,
        author_database_incarnation: incarnation,
        author_local_mutation_position: Lsn(position),
        table_generation: generation,
        lineage_root,
        attestation: identity.sign_lineage(&bytes),
    }
}

fn accepted_delete_record_for_lineage(
    db: &Database,
    key: &NaturalKey,
    lineage: &crate::protocol::WireRowLineage,
) {
    let record = DurableLineageRecord {
        table: "events".to_string(),
        natural_key: key.clone(),
        table_generation: lineage.table_generation,
        local_row_id: Some(RowId(1)),
        locally_created: false,
        lineage_root: lineage.lineage_root.clone(),
        lineage_attestation: lineage.attestation.clone(),
        author_node_id: Some(lineage.author_node_id.clone()),
        author_database_incarnation: Some(lineage.author_database_incarnation.to_hex()),
        author_local_mutation_position: lineage.author_local_mutation_position.0,
        delete_lsn: 4,
        delete_obligation: DurableDeleteObligation::Accepted,
        accepted_hub_lsn: Some(4),
        bound_hub_node_id: Some("hub".to_string()),
        purge_frontier: None,
    };
    db.persistence
        .as_ref()
        .unwrap()
        .flush_config_value(
            &Database::durable_lineage_config_key("events", key, lineage.table_generation),
            &record,
        )
        .unwrap();
}

#[test]
fn accepted_delete_strict_wrappers_reject_a_terminated_member_without_returning_siblings() {
    let (_temp, db, key) = accepted_delete_fixture();
    let tenant = TenantId::from("strict-accepted-delete-test");
    let identity = crate::identity::FabricIdentity::generate();
    let terminated = signed_lineage(&db, &tenant, &key, 11, &identity);
    accepted_delete_record_for_lineage(&db, &key, &terminated);
    let sibling_key = NaturalKey::from_pairs(vec![
        ("id".to_string(), Value::Int64(2)),
        ("scope".to_string(), Value::Text("sibling".to_string())),
    ])
    .unwrap();
    let sibling = signed_lineage(&db, &tenant, &sibling_key, 12, &identity);
    let sibling_row = RowChange {
        table: "events".to_string(),
        natural_key: sibling_key.clone(),
        values: HashMap::from([
            ("id".to_string(), Value::Int64(2)),
            ("scope".to_string(), Value::Text("sibling".to_string())),
        ]),
        deleted: false,
        lsn: Lsn(5),
        created_at: None,
    };
    let changes = ChangeSet {
        rows: vec![accepted_delete_row(key.clone(), Lsn(5), false), sibling_row],
        ..ChangeSet::default()
    };
    let lineages = vec![
        ("events".to_string(), key, Lsn(5), terminated),
        ("events".to_string(), sibling_key, Lsn(5), sibling),
    ];
    let fixture_operation_reader_30 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    assert!(
        db.reject_dependency_complete_accepted_lineage_replays(
            &tenant,
            changes.clone(),
            &lineages,
            fixture_operation_reader_30.clone()
        )
        .is_err()
    );
    let fixture_operation_reader_31 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    assert!(
        db.reject_accepted_lineage_replays(
            &tenant,
            changes,
            &lineages,
            fixture_operation_reader_31.clone()
        )
        .is_err()
    );
}

#[test]
fn dependency_complete_outbound_keeps_ddl_only_migrations_atomic_without_foreign_keys() {
    let db = Database::open_memory();
    let changes = ChangeSet {
        ddl: vec![create_table()],
        ddl_lsn: vec![Lsn(41)],
        ..ChangeSet::default()
    };
    let fixture_operation_reader_1 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");
    let mut fixture_operation_copy_1 =
        contextdb_engine::vector_read_memory::admission(fixture_operation_reader_1.clone());

    let units = db
        .dependency_complete_outbound_units(
            changes,
            Lsn(0),
            &mut fixture_operation_copy_1,
            fixture_operation_reader_1.clone(),
        )
        .expect("DDL-only migration is a valid authenticated atomic unit");
    assert_eq!(units.len(), 1);
    assert!(units[0].dependency_complete);
    assert_eq!(units[0].changes.ddl_lsn, vec![Lsn(41)]);
    db.validate_dependency_complete_unit(&units[0].changes)
        .expect("DDL-only migration does not require a foreign-key component");
}

/// A policy-only vector ALTER is its own authored transaction, and the
/// outbound builder ships it as its own unit even when the same table's
/// vectors were written just before and just after it. The receiver
/// therefore applies the policy in place and the vector rows through the
/// ordinary path, which keeps every serving graph.
#[test]
fn dependency_complete_outbound_isolates_a_policy_alter_from_the_same_tables_vectors() {
    let db = Database::open_memory();
    let alter = DdlChange::AlterTable {
        name: "policy_items".to_string(),
        columns: vec![
            ("id".to_string(), "UUID PRIMARY KEY".to_string()),
            ("scope".to_string(), "TEXT NOT NULL".to_string()),
            (
                "embedding".to_string(),
                "VECTOR(3) PARTITION BY (scope) SEARCH_MODE AUTO".to_string(),
            ),
        ],
        constraints: Vec::new(),
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    };
    let vector = |row: u64, lsn: u64| VectorChange {
        index: VectorIndexRef::new("policy_items", "embedding"),
        row_id: RowId(row),
        vector: vec![1.0, 0.0, 0.0],
        lsn: Lsn(lsn),
    };
    let changes = ChangeSet {
        vectors: vec![vector(1, 40), vector(2, 42)],
        ddl: vec![alter],
        ddl_lsn: vec![Lsn(41)],
        ..ChangeSet::default()
    };
    let reader = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");
    let mut copy = contextdb_engine::vector_read_memory::admission(reader.clone());

    let units = db
        .dependency_complete_outbound_units(changes, Lsn(0), &mut copy, reader.clone())
        .expect("build the outbound units");
    let schema_units = units
        .iter()
        .filter(|unit| !unit.changes.ddl.is_empty())
        .collect::<Vec<_>>();
    assert_eq!(schema_units.len(), 1, "{units:?}");
    assert!(schema_units[0].dependency_complete);
    assert_eq!(schema_units[0].changes.ddl_lsn, vec![Lsn(41)]);
    assert!(
        schema_units[0].changes.vectors.is_empty() && schema_units[0].changes.rows.is_empty(),
        "the policy ALTER travels without the table's vector rows: {units:?}"
    );
    let mut shipped = units
        .iter()
        .flat_map(|unit| unit.changes.vectors.iter().map(|vector| vector.lsn))
        .collect::<Vec<_>>();
    shipped.sort();
    assert_eq!(
        shipped,
        vec![Lsn(40), Lsn(42)],
        "every vector row still ships"
    );
}

#[test]
fn dependency_complete_outbound_splits_unrelated_data_from_schema_migration() {
    let db = Database::open_memory();
    let migration_row = RowChange {
        table: "events".to_string(),
        natural_key: NaturalKey::single("id".to_string(), Value::Int64(1)),
        values: HashMap::from([("id".to_string(), Value::Int64(1))]),
        deleted: false,
        lsn: Lsn(41),
        created_at: None,
    };
    let unrelated_row = RowChange {
        table: "notes".to_string(),
        natural_key: NaturalKey::single("id".to_string(), Value::Int64(2)),
        values: HashMap::from([("id".to_string(), Value::Int64(2))]),
        deleted: false,
        lsn: Lsn(42),
        created_at: None,
    };
    let changes = ChangeSet {
        rows: vec![migration_row.clone(), unrelated_row.clone()],
        ddl: vec![create_table()],
        ddl_lsn: vec![Lsn(41)],
        ..ChangeSet::default()
    };
    let fixture_operation_reader_2 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");
    let mut fixture_operation_copy_2 =
        contextdb_engine::vector_read_memory::admission(fixture_operation_reader_2.clone());

    let units = db
        .dependency_complete_outbound_units(
            changes,
            Lsn(0),
            &mut fixture_operation_copy_2,
            fixture_operation_reader_2.clone(),
        )
        .expect("partition the schema migration from ordinary traffic");
    assert_eq!(units.len(), 2);
    assert!(units[0].dependency_complete);
    assert_eq!(units[0].changes.rows, vec![migration_row]);
    assert_eq!(units[0].changes.ddl_lsn, vec![Lsn(41)]);
    assert!(!units[1].dependency_complete);
    assert_eq!(units[1].changes.rows, vec![unrelated_row]);

    let invalid_mixture = ChangeSet {
        rows: units[1].changes.rows.clone(),
        ddl: units[0].changes.ddl.clone(),
        ddl_lsn: units[0].changes.ddl_lsn.clone(),
        ..ChangeSet::default()
    };
    assert!(
        db.validate_dependency_complete_unit(&invalid_mixture)
            .is_err(),
        "a sender cannot use a DDL sidecar to bypass ordinary dependency validation"
    );
}

#[test]
fn failed_received_schema_preparation_registers_nothing_and_cannot_publish() {
    let db = Database::open_memory();
    let changes = ChangeSet {
        ddl: vec![create_table()],
        // This malformed cardinality makes source-order preparation fail
        // before there is a stage to register or a projection to publish.
        ddl_lsn: Vec::new(),
        ..ChangeSet::default()
    };
    let before_ddl = db.ddl_log.read().clone();
    let received_schema_read_memory_2 = db
        .declared_reader_memory()
        .expect("admit the received-schema operation reader");
    let mut received_schema_copy_owner_2 =
        contextdb_engine::vector_read_memory::admission(received_schema_read_memory_2.clone());

    let error = db.prepare_received_schema_stage(
        Lsn(9),
        &changes,
        &received_context(),
        &[],
        &mut received_schema_copy_owner_2,
        received_schema_read_memory_2,
    );

    assert!(error.is_err());
    assert!(db.received_schema_stages.lock().is_empty());
    assert_eq!(*db.ddl_log.read(), before_ddl);
}

#[test]
fn failed_received_schema_preparation_does_not_advance_live_row_allocator() {
    let db = Database::open_memory();
    let before = db.relational_store.next_row_id();
    let changes = ChangeSet {
        ddl: vec![create_table()],
        // The received provenance is deliberately incomplete, so
        // preparation reaches no durable/publication phase.
        ddl_lsn: Vec::new(),
        rows: vec![RowChange {
            table: "events".to_string(),
            natural_key: NaturalKey::single("id".to_string(), Value::Int64(1)),
            values: HashMap::from([("id".to_string(), Value::Int64(1))]),
            deleted: false,
            lsn: Lsn(41),
            created_at: None,
        }],
        ..ChangeSet::default()
    };
    let received_schema_read_memory_3 = db
        .declared_reader_memory()
        .expect("admit the received-schema operation reader");
    let mut received_schema_copy_owner_3 =
        contextdb_engine::vector_read_memory::admission(received_schema_read_memory_3.clone());

    assert!(
        db.prepare_received_schema_stage(
            Lsn(9),
            &changes,
            &received_context(),
            &[],
            &mut received_schema_copy_owner_3,
            received_schema_read_memory_3
        )
        .is_err()
    );
    assert_eq!(db.relational_store.next_row_id(), before);
}

#[test]
fn detached_adjudication_assigns_receiver_mvcc_and_remaps_remote_vector_owner() {
    let db = Database::open_memory();
    let ddl = DdlChange::CreateTable {
        name: "embeddings".to_string(),
        columns: vec![
            ("id".to_string(), "INTEGER PRIMARY KEY".to_string()),
            ("embedding".to_string(), "VECTOR(2)".to_string()),
        ],
        constraints: Vec::new(),
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    };
    let received = crate::protocol::ReceivedDdlContext {
        tenant_id: TenantId::from("tenant-a"),
        source_node_id: "authenticated-source".to_string(),
        source_incarnation: Incarnation(7),
        entries: vec![crate::protocol::ReceivedDdlEntry {
            source_ddl_lsn: Lsn(41),
            ordinal: 0,
            table: Some("embeddings".to_string()),
            table_generation: Some(3),
            digest: vec![1],
            author: None,
        }],
    };
    let key = NaturalKey::single("id".to_string(), Value::Int64(7));
    let received_schema_read_memory_4 = db
        .declared_reader_memory()
        .expect("admit the received-schema operation reader");
    let mut received_schema_copy_owner_4 =
        contextdb_engine::vector_read_memory::admission(received_schema_read_memory_4.clone());

    let stage = db
        .prepare_received_schema_stage(
            Lsn(9),
            &ChangeSet {
                ddl: vec![ddl],
                ddl_lsn: vec![Lsn(41)],
                rows: vec![RowChange {
                    table: "embeddings".to_string(),
                    natural_key: key.clone(),
                    values: HashMap::from([
                        ("id".to_string(), Value::Int64(7)),
                        ("embedding".to_string(), Value::Vector(vec![0.0, 1.0])),
                    ]),
                    deleted: false,
                    lsn: Lsn(77),
                    created_at: None,
                }],
                vectors: vec![VectorChange {
                    index: VectorIndexRef::new("embeddings", "embedding"),
                    row_id: RowId(999),
                    vector: vec![0.0, 1.0],
                    lsn: Lsn(77),
                }],
                ..ChangeSet::default()
            },
            &received,
            &[(
                "embeddings".to_string(),
                key,
                Lsn(77),
                lineage("origin", 501, 3),
            )],
            &mut received_schema_copy_owner_4,
            received_schema_read_memory_4,
        )
        .expect("detached adjudication");
    db.publish_received_schema_stage(stage);

    let row = db.relational_store.tables.read()["embeddings"]
        .iter()
        .find(|row| row.deleted_tx.is_none())
        .cloned()
        .expect("received row");
    assert_ne!(
        row.created_tx,
        TxId(77),
        "wire LSN is never receiver MVCC identity"
    );
    let vector_fixture_reader_1 = db
        .declared_reader_memory()
        .expect("admit the vector fixture reader");
    let mut vector_fixture_copy_1 =
        contextdb_engine::vector_read_memory::admission(vector_fixture_reader_1.clone());

    let vectors = db
        .vector_store
        .all_entries(&mut vector_fixture_copy_1)
        .expect("read or publish the admitted vector fixture");
    assert_eq!(vectors.len(), 1);
    assert_eq!(
        vectors[0].row_id, row.row_id,
        "vector follows the local owner"
    );
    assert_ne!(vectors[0].row_id, RowId(999));
}

#[test]
fn detached_stage_restamps_only_affected_row_edge_and_vector_mvcc() {
    let db = Database::open_memory();
    let params = HashMap::new();
    db.execute(
        "CREATE TABLE embeddings (id INTEGER PRIMARY KEY, embedding VECTOR(2))",
        &params,
    )
    .unwrap();
    db.execute("INSERT INTO embeddings (id) VALUES (1)", &params)
        .unwrap();
    let old_row = db.relational_store.tables.read()["embeddings"][0].clone();
    let index = VectorIndexRef::new("embeddings", "embedding");
    let old_vector = VectorEntry {
        index: index.clone(),
        row_id: old_row.row_id,
        vector: vec![1.0, 0.0],
        created_tx: TxId(13),
        deleted_tx: None,
        lsn: Lsn(13),
    };
    db.vector_store
        .insert_loaded_vector(old_vector.clone(), db.accountant.vector_budget().unwrap())
        .expect("read or publish the admitted vector fixture");
    let old_edge = AdjEntry {
        source: uuid::Uuid::from_u128(10),
        target: uuid::Uuid::from_u128(11),
        edge_type: "old".to_string(),
        properties: HashMap::new(),
        created_tx: TxId(12),
        deleted_tx: None,
        lsn: Lsn(12),
    };
    db.accountant
        .try_allocate(contextdb_graph::GraphAdjEntry::from(old_edge.clone()).estimated_bytes())
        .expect("fund the graph fixture that finalization refunds");
    db.graph_store.apply_inserts(vec![old_edge.clone()]);
    let ddl = DdlChange::AlterTable {
        name: "embeddings".to_string(),
        columns: vec![
            ("id".to_string(), "INTEGER PRIMARY KEY".to_string()),
            ("embedding".to_string(), "VECTOR(2)".to_string()),
        ],
        constraints: Vec::new(),
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    };
    let received = received_single_ddl(&ddl, 3);
    let key = NaturalKey::single("id".to_string(), Value::Int64(2));
    let changes = ChangeSet {
        ddl: vec![ddl],
        ddl_lsn: vec![Lsn(41)],
        rows: vec![RowChange {
            table: "embeddings".to_string(),
            natural_key: key.clone(),
            values: HashMap::from([
                ("id".to_string(), Value::Int64(2)),
                ("embedding".to_string(), Value::Vector(vec![0.0, 1.0])),
            ]),
            deleted: false,
            lsn: Lsn(42),
            created_at: None,
        }],
        edges: vec![EdgeChange {
            source: uuid::Uuid::from_u128(20),
            target: uuid::Uuid::from_u128(21),
            edge_type: "new".to_string(),
            properties: HashMap::new(),
            lsn: Lsn(42),
        }],
        vectors: vec![VectorChange {
            index: index.clone(),
            row_id: RowId(999),
            vector: vec![0.0, 1.0],
            lsn: Lsn(42),
        }],
    };
    let arrivals = HashMap::new();
    let received_schema_read_memory_5 = db
        .declared_reader_memory()
        .expect("admit the received-schema operation reader");
    let mut received_schema_copy_owner_5 =
        contextdb_engine::vector_read_memory::admission(received_schema_read_memory_5.clone());

    let stage = db
        .prepare_received_schema_stage_with_adjudication(
            Lsn(99),
            &changes,
            &received,
            &[(
                "embeddings".to_string(),
                key,
                Lsn(42),
                lineage("origin", 501, 3),
            )],
            ReceivedSchemaAdjudicationInputs {
                push_outcome_key: None,
                apply_role: SyncApplyRole::PullLeg,
                arrivals: &arrivals,
                adoption: SyncAdoption::Continuing,
                receipt: None,
                dependency_complete: false,
                terminal_refusal_context: None,
                hub_local_author: None,
                unattested_policies: None,
                receiver_tx: TxId(37),
            },
            &mut received_schema_copy_owner_5,
            received_schema_read_memory_5,
        )
        .unwrap();
    db.publish_received_schema_stage(stage);

    let rows = db.relational_store.tables.read();
    let new_row = rows["embeddings"]
        .iter()
        .find(|row| row.values.get("id") == Some(&Value::Int64(2)))
        .unwrap();
    let preserved_row = rows["embeddings"]
        .iter()
        .find(|row| row.row_id == old_row.row_id)
        .unwrap();
    assert_eq!(new_row.created_tx, TxId(37));
    assert_eq!(new_row.lsn, Lsn(99));
    assert_eq!(preserved_row.created_tx, old_row.created_tx);
    assert_eq!(preserved_row.deleted_tx, old_row.deleted_tx);
    let edges = db.graph_store.forward_adj.read();
    let new_edge = edges[&uuid::Uuid::from_u128(20)]
        .iter()
        .find(|edge| edge.edge_type == "new")
        .unwrap();
    let preserved_edge = edges[&old_edge.source]
        .iter()
        .find(|edge| edge.edge_type == "old")
        .unwrap();
    assert_eq!(new_edge.created_tx, TxId(37));
    assert_eq!(new_edge.lsn, Lsn(99));
    assert_eq!(preserved_edge.created_tx, old_edge.created_tx);
    assert_eq!(preserved_edge.deleted_tx, old_edge.deleted_tx);
    let vector_fixture_reader_3 = db
        .declared_reader_memory()
        .expect("admit the vector fixture reader");
    let mut vector_fixture_copy_3 =
        contextdb_engine::vector_read_memory::admission(vector_fixture_reader_3.clone());

    let vectors = db
        .vector_store
        .all_entries(&mut vector_fixture_copy_3)
        .expect("read or publish the admitted vector fixture");
    let new_vector = vectors
        .iter()
        .find(|entry| entry.row_id == new_row.row_id)
        .unwrap();
    let preserved_vector = vectors
        .iter()
        .find(|entry| entry.row_id == old_vector.row_id)
        .unwrap();
    assert_eq!(new_vector.created_tx, TxId(37));
    assert_eq!(new_vector.lsn, Lsn(99));
    assert_eq!(preserved_vector.created_tx, old_vector.created_tx);
    assert_eq!(preserved_vector.deleted_tx, old_vector.deleted_tx);
}

#[test]
fn detached_drop_then_create_starts_same_named_table_without_old_rows_or_vectors() {
    let db = Database::open_memory();
    let params = HashMap::new();
    db.execute(
        "CREATE TABLE events (id INTEGER PRIMARY KEY, embedding VECTOR(2))",
        &params,
    )
    .unwrap();
    db.execute(
        "INSERT INTO events (id, embedding) VALUES (1, [1.0, 0.0])",
        &params,
    )
    .unwrap();
    let index = VectorIndexRef::new("events", "embedding");
    let old_row_id = db.relational_store.tables.read()["events"][0].row_id;
    db.vector_store
        .insert_loaded_vector(
            VectorEntry {
                index: index.clone(),
                row_id: old_row_id,
                vector: vec![1.0, 0.0],
                created_tx: TxId(1),
                deleted_tx: None,
                lsn: Lsn(1),
            },
            db.accountant.vector_budget().unwrap(),
        )
        .expect("read or publish the admitted vector fixture");
    let ddl = vec![
        DdlChange::DropTable {
            name: "events".to_string(),
        },
        DdlChange::CreateTable {
            name: "events".to_string(),
            columns: vec![("id".to_string(), "INTEGER PRIMARY KEY".to_string())],
            constraints: Vec::new(),
            foreign_keys: Vec::new(),
            composite_foreign_keys: Vec::new(),
            composite_unique: Vec::new(),
        },
    ];
    let received = crate::protocol::ReceivedDdlContext {
        tenant_id: TenantId::from("tenant-a"),
        source_node_id: "authenticated-source".to_string(),
        source_incarnation: Incarnation(7),
        entries: ddl
            .iter()
            .enumerate()
            .map(|(ordinal, change)| crate::protocol::ReceivedDdlEntry {
                source_ddl_lsn: Lsn(41),
                ordinal: ordinal as u32,
                table: Database::ddl_affected_table(change).map(str::to_string),
                table_generation: Some(4),
                digest: vec![ordinal as u8],
                author: None,
            })
            .collect(),
    };
    let received_schema_read_memory_6 = db
        .declared_reader_memory()
        .expect("admit the received-schema operation reader");
    let mut received_schema_copy_owner_6 =
        contextdb_engine::vector_read_memory::admission(received_schema_read_memory_6.clone());

    let stage = db
        .prepare_received_schema_stage(
            Lsn(9),
            &ChangeSet {
                ddl,
                ddl_lsn: vec![Lsn(41), Lsn(41)],
                ..ChangeSet::default()
            },
            &received,
            &[],
            &mut received_schema_copy_owner_6,
            received_schema_read_memory_6,
        )
        .expect("drop then create detached projection");
    db.publish_received_schema_stage(stage);

    assert!(db.table_meta("events").is_some());
    assert!(db.relational_store.tables.read()["events"].is_empty());
    let vector_fixture_reader_5 = db
        .declared_reader_memory()
        .expect("admit the vector fixture reader");
    let mut vector_fixture_copy_5 =
        contextdb_engine::vector_read_memory::admission(vector_fixture_reader_5.clone());

    assert!(
        db.vector_store
            .all_entries(&mut vector_fixture_copy_5)
            .expect("read or publish the admitted vector fixture")
            .iter()
            .all(|entry| entry.index.table != "events")
    );
}

#[test]
fn source_order_semantics_refuse_forward_references_and_remove_table_bound_state() {
    let db = Database::open_memory();
    let event_before_table = vec![
        DdlChange::CreateEventType {
            name: "event".to_string(),
            trigger: "INSERT".to_string(),
            table: "events".to_string(),
        },
        create_table(),
    ];
    assert!(
        db.project_received_schema_source_order(&event_before_table)
            .is_err()
    );
    let trigger_before_table = vec![create_trigger(), create_table()];
    assert!(
        db.project_received_schema_source_order(&trigger_before_table)
            .is_err()
    );

    let create_then_drop = vec![
        create_table(),
        DdlChange::CreateEventType {
            name: "event".to_string(),
            trigger: "INSERT".to_string(),
            table: "events".to_string(),
        },
        create_trigger(),
        DdlChange::DropTable {
            name: "events".to_string(),
        },
    ];
    let projected = db
        .project_received_schema_source_order(&create_then_drop)
        .expect("create then drop is a valid source lifecycle");
    assert!(projected.trigger.is_empty());
    assert!(!projected.tables.contains_key("events"));
}

#[test]
fn source_order_uses_table_and_foreign_key_validation_for_each_ddl_item() {
    let db = Database::open_memory();
    let invalid_index = vec![
        create_table(),
        DdlChange::CreateIndex {
            table: "events".to_string(),
            name: "missing_column".to_string(),
            columns: vec![("missing".to_string(), SortDirection::Asc)],
        },
    ];
    assert!(
        db.project_received_schema_source_order(&invalid_index)
            .is_err()
    );

    let invalid_foreign_key = vec![DdlChange::CreateTable {
        name: "child".to_string(),
        columns: vec![
            ("id".to_string(), "INTEGER".to_string()),
            ("parent_id".to_string(), "INTEGER".to_string()),
        ],
        constraints: Vec::new(),
        foreign_keys: vec![SingleColumnForeignKey {
            child_column: "parent_id".to_string(),
            parent_table: "parent".to_string(),
            parent_column: "id".to_string(),
        }],
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    }];
    assert!(
        db.project_received_schema_source_order(&invalid_foreign_key)
            .is_err()
    );
}

#[test]
fn prepared_memory_swap_reserves_rolls_back_and_settles_replacement_deltas() {
    let over_budget = Arc::new(MemoryAccountant::with_budget(4));
    over_budget.try_allocate(4).unwrap();
    let before = over_budget.usage().used;
    assert!(PreparedMemorySwap::prepare(over_budget.clone().into(), 0, 1, 0).is_err());
    assert_eq!(over_budget.usage().used, before);

    let accountant = Arc::new(MemoryAccountant::with_budget(64));
    let abandoned = PreparedMemorySwap::prepare(accountant.clone().into(), 0, 8, 0).unwrap();
    assert_eq!(accountant.usage().used, 8);
    drop(abandoned);
    assert_eq!(accountant.usage().used, 0);

    let mut published = PreparedMemorySwap::prepare(accountant.clone().into(), 0, 8, 0).unwrap();
    published.commit_after_swap();
    assert_eq!(accountant.usage().used, 8);
}

/// The release path's schema-less fallback answers the SAME number
/// admission charged.
///
/// Admission always has the table's schema; a release sometimes does not.
/// Both paths must account for values, side maps and one posting per
/// declared index, or the difference remains charged for the store's life.
/// There is no reachable statement that drops a table inside the
/// transaction that filled it, so this holds the two functions to each
/// other directly rather than through a door.
#[test]
fn row_version_bytes_match_the_schema_less_fallback() {
    let row = VersionedRow {
        row_id: RowId(7),
        values: HashMap::from([
            ("id".to_string(), Value::Uuid(uuid::Uuid::from_u128(9))),
            ("body".to_string(), Value::Text("x".repeat(300))),
            ("seq".to_string(), Value::Int64(4)),
            ("shape".to_string(), Value::Vector(vec![0.5; 16])),
        ]),
        created_tx: TxId(1),
        deleted_tx: None,
        lsn: Lsn(1),
        created_at: None,
    };
    // A table whose columns are exactly this row's, declaring no index --
    // which is what a table with no schema left has.
    let meta = TableMeta {
        columns: vec![
            rough_sync_column_def("id", "UUID"),
            rough_sync_column_def("body", "TEXT"),
            rough_sync_column_def("seq", "INTEGER"),
            rough_sync_column_def("shape", "VECTOR(16)"),
        ],
        ..TableMeta::default()
    };
    assert_eq!(
        fallback_retained_row_bytes("notes", &row),
        retained_row_bytes_for_meta("notes", &row.values, &meta, true),
        "a release that cannot read the schema must still hand back what admission took"
    );
}

#[test]
fn schema_stage_rejects_missing_duplicate_or_mismatched_lineage_without_mutation() {
    // 4 KiB over the schema: the single-column PRIMARY KEY allocates a
    // fixed 64 KiB exact-probe bitmap for the life of the index, which it
    // always did and which the accountant now sees.
    let accountant = Arc::new(MemoryAccountant::with_budget(
        contextdb_core::EXACT_PROBE_FILTER_BYTES + 4096,
    ));
    let db = Database::open_memory_with_accountant(accountant.clone());
    db.execute(
        "CREATE TABLE lineage_rows (id INTEGER PRIMARY KEY)",
        &HashMap::new(),
    )
    .unwrap();
    let baseline = accountant.usage().used;
    let key = NaturalKey::single("id".to_string(), Value::Int64(1));
    let changes = ChangeSet {
        rows: vec![RowChange {
            table: "lineage_rows".to_string(),
            natural_key: key.clone(),
            values: HashMap::from([("id".to_string(), Value::Int64(1))]),
            deleted: false,
            lsn: Lsn(50),
            created_at: None,
        }],
        ..ChangeSet::default()
    };
    let received = crate::protocol::ReceivedDdlContext {
        tenant_id: TenantId::from("tenant-a"),
        source_node_id: "relay-node".to_string(),
        source_incarnation: Incarnation(7),
        entries: Vec::new(),
    };
    let direct = (
        "lineage_rows".to_string(),
        key.clone(),
        Lsn(50),
        lineage("origin", 501, 1),
    );
    let received_schema_read_memory_7 = db
        .declared_reader_memory()
        .expect("admit the received-schema operation reader");
    let mut received_schema_copy_owner_7 =
        contextdb_engine::vector_read_memory::admission(received_schema_read_memory_7.clone());

    assert!(
        db.prepare_received_schema_stage(
            Lsn(9),
            &changes,
            &received,
            &[],
            &mut received_schema_copy_owner_7,
            received_schema_read_memory_7
        )
        .is_err()
    );
    let received_schema_read_memory_8 = db
        .declared_reader_memory()
        .expect("admit the received-schema operation reader");
    let mut received_schema_copy_owner_8 =
        contextdb_engine::vector_read_memory::admission(received_schema_read_memory_8.clone());

    assert!(
        db.prepare_received_schema_stage(
            Lsn(9),
            &changes,
            &received,
            &[direct.clone(), direct.clone()],
            &mut received_schema_copy_owner_8,
            received_schema_read_memory_8
        )
        .is_err()
    );
    let received_schema_read_memory_9 = db
        .declared_reader_memory()
        .expect("admit the received-schema operation reader");
    let mut received_schema_copy_owner_9 =
        contextdb_engine::vector_read_memory::admission(received_schema_read_memory_9.clone());

    assert!(
        db.prepare_received_schema_stage(
            Lsn(9),
            &changes,
            &received,
            &[(
                "wrong_table".to_string(),
                key,
                Lsn(50),
                lineage("origin", 501, 1)
            )],
            &mut received_schema_copy_owner_9,
            received_schema_read_memory_9
        )
        .is_err()
    );
    assert!(db.relational_store.tables.read()["lineage_rows"].is_empty());
    assert_eq!(accountant.usage().used, baseline);
}

#[test]
fn staged_vector_replacement_releases_retired_hnsw_and_negative_delta() {
    // Fund the counted preparation controls as well as the vector fixture.
    // The invariant below is exact retirement back to the opening balance.
    let accountant = Arc::new(MemoryAccountant::with_budget(8 * 1024));
    let db = Database::open_memory_with_accountant(accountant.clone());
    let baseline = accountant.usage().used;
    let index = VectorIndexRef::new("retired", "embedding");
    db.vector_store
        .register_index(
            index.clone(),
            2,
            contextdb_core::VectorQuantization::F32,
            contextdb_vector::MemoryBudgetOwner::clone(&db.finalization.vector_accountant),
        )
        .expect("read or publish the admitted vector fixture");
    accountant
        .try_allocate(contextdb_vector::stored_vector_resident_bytes(
            2,
            contextdb_core::VectorQuantization::F32,
        ))
        .unwrap();
    db.vector_store
        .insert_loaded_vector(
            VectorEntry {
                index: index.clone(),
                row_id: RowId(1),
                vector: vec![1.0, 0.0],
                created_tx: TxId(1),
                deleted_tx: None,
                lsn: Lsn(1),
            },
            contextdb_vector::MemoryBudgetOwner::clone(&db.finalization.vector_accountant),
        )
        .expect("read or publish the admitted vector fixture");
    let state = db.vector_store.try_state(&index).unwrap();
    accountant.try_allocate(64).unwrap();
    state.set_hnsw_bytes(64);
    let received_schema_read_memory_10 = db
        .declared_reader_memory()
        .expect("admit the received-schema operation reader");
    let mut received_schema_copy_owner_10 =
        contextdb_engine::vector_read_memory::admission(received_schema_read_memory_10.clone());

    let stage = db
        .prepare_received_schema_stage(
            Lsn(9),
            &ChangeSet::default(),
            &crate::protocol::ReceivedDdlContext {
                tenant_id: TenantId::from("tenant-a"),
                source_node_id: "authenticated-source".to_string(),
                source_incarnation: Incarnation(7),
                entries: Vec::new(),
            },
            &[],
            &mut received_schema_copy_owner_10,
            received_schema_read_memory_10,
        )
        .unwrap();
    assert_eq!(db.tx_mgr.commit_index_len(), 0);
    db.publish_received_schema_stage(stage);
    // This fixture bypasses the transaction manager: publication funds
    // its future commit-index entry, but no such entry is installed here.
    assert_eq!(db.tx_mgr.commit_index_len(), 0);
    accountant.release(crate::composite_store::COMMIT_INDEX_ENTRY_BYTES);
    drop(state);
    let vector_fixture_reader_8 = db
        .declared_reader_memory()
        .expect("admit the vector fixture reader");
    let mut vector_fixture_copy_8 =
        contextdb_engine::vector_read_memory::admission(vector_fixture_reader_8.clone());

    assert!(
        db.vector_store
            .all_entries(&mut vector_fixture_copy_8)
            .expect("read or publish the admitted vector fixture")
            .is_empty()
    );
    assert_eq!(accountant.usage().used, baseline);
}

#[test]
fn staged_publication_moves_complete_replacements_across_derived_handles() {
    // The raw codec fixture owns this explicitly declared operation.
    let _fixture_decode_scope = crate::read_image_memory::ImageMemory::enter_writer_operation(
        crate::memory_accounting::AccountantHandle::standalone(None, true).unwrap(),
    )
    .unwrap();

    let db = Database::open_memory();
    let params = HashMap::new();
    db.execute("CREATE TABLE kept (id INTEGER PRIMARY KEY)", &params)
        .unwrap();
    db.execute("CREATE TABLE dropped (id INTEGER PRIMARY KEY)", &params)
        .unwrap();
    db.execute("INSERT INTO kept (id) VALUES (1)", &params)
        .unwrap();
    let graph_entry = AdjEntry {
        source: uuid::Uuid::nil(),
        target: uuid::Uuid::from_u128(1),
        edge_type: "related".to_string(),
        properties: HashMap::new(),
        created_tx: TxId(1),
        deleted_tx: None,
        lsn: Lsn(1),
    };
    db.accountant
        .try_allocate(contextdb_graph::GraphAdjEntry::from(graph_entry.clone()).estimated_bytes())
        .expect("fund the graph fixture that finalization refunds");
    db.graph_store.apply_inserts(vec![graph_entry]);
    let vector_index = VectorIndexRef::new("kept", "embedding");
    db.vector_store
        .register_index(
            vector_index.clone(),
            2,
            contextdb_core::VectorQuantization::F32,
            db.accountant.vector_budget().unwrap(),
        )
        .expect("read or publish the admitted vector fixture");
    db.vector_store
        .insert_loaded_vector(
            VectorEntry {
                index: vector_index.clone(),
                row_id: RowId(1),
                vector: vec![1.0, 0.0],
                created_tx: TxId(1),
                deleted_tx: None,
                lsn: Lsn(1),
            },
            db.accountant.vector_budget().unwrap(),
        )
        .expect("read or publish the admitted vector fixture");

    let ddl = vec![
        DdlChange::AlterTable {
            name: "kept".to_string(),
            columns: vec![
                ("id".to_string(), "INTEGER PRIMARY KEY".to_string()),
                ("embedding".to_string(), "VECTOR(2)".to_string()),
            ],
            constraints: Vec::new(),
            foreign_keys: Vec::new(),
            composite_foreign_keys: Vec::new(),
            composite_unique: Vec::new(),
        },
        create_table(),
        DdlChange::CreateEventType {
            name: "event".to_string(),
            trigger: "INSERT".to_string(),
            table: "events".to_string(),
        },
        create_trigger(),
        DdlChange::DropTable {
            name: "events".to_string(),
        },
        DdlChange::DropTable {
            name: "dropped".to_string(),
        },
    ];
    let received = crate::protocol::ReceivedDdlContext {
        tenant_id: TenantId::from("tenant-a"),
        source_node_id: "authenticated-source".to_string(),
        source_incarnation: Incarnation(7),
        entries: ddl
            .iter()
            .enumerate()
            .map(|(ordinal, change)| crate::protocol::ReceivedDdlEntry {
                source_ddl_lsn: Lsn(41),
                ordinal: ordinal as u32,
                table: Database::ddl_affected_table(change).map(str::to_string),
                table_generation: Some(4),
                digest: vec![ordinal as u8],
                author: None,
            })
            .collect(),
    };
    let changes = ChangeSet {
        ddl,
        ddl_lsn: vec![Lsn(41); 6],
        rows: vec![
            RowChange {
                table: "kept".to_string(),
                natural_key: NaturalKey::single("id".to_string(), Value::Int64(2)),
                values: HashMap::from([("id".to_string(), Value::Int64(2))]),
                deleted: false,
                lsn: Lsn(42),
                created_at: None,
            },
            RowChange {
                table: "kept".to_string(),
                natural_key: NaturalKey::single("id".to_string(), Value::Int64(3)),
                values: HashMap::from([("id".to_string(), Value::Int64(3))]),
                deleted: false,
                lsn: Lsn(43),
                created_at: None,
            },
        ],
        edges: vec![EdgeChange {
            source: uuid::Uuid::from_u128(2),
            target: uuid::Uuid::from_u128(3),
            edge_type: "received".to_string(),
            properties: HashMap::new(),
            lsn: Lsn(42),
        }],
        vectors: vec![
            VectorChange {
                index: vector_index.clone(),
                row_id: RowId(2),
                vector: vec![0.0, 1.0],
                lsn: Lsn(42),
            },
            VectorChange {
                index: vector_index.clone(),
                row_id: RowId(3),
                vector: vec![0.5, 0.5],
                lsn: Lsn(43),
            },
        ],
    };
    let received_schema_read_memory_11 = db
        .declared_reader_memory()
        .expect("admit the received-schema operation reader");
    let mut received_schema_copy_owner_11 =
        contextdb_engine::vector_read_memory::admission(received_schema_read_memory_11.clone());

    let stage = db
        .prepare_received_schema_stage(
            Lsn(9),
            &changes,
            &received,
            &[
                (
                    "kept".to_string(),
                    NaturalKey::single("id".to_string(), Value::Int64(2)),
                    Lsn(42),
                    lineage("direct-author", 501, 4),
                ),
                (
                    "kept".to_string(),
                    NaturalKey::single("id".to_string(), Value::Int64(3)),
                    Lsn(43),
                    lineage("original-author-through-relay", 502, 4),
                ),
            ],
            &mut received_schema_copy_owner_11,
            received_schema_read_memory_11,
        )
        .expect("all fallible preparation completes before publication");
    let direct_identity = sync_identity_key(
        "kept",
        &NaturalKey::single("id".to_string(), Value::Int64(2)),
    );
    let relayed_identity = sync_identity_key(
        "kept",
        &NaturalKey::single("id".to_string(), Value::Int64(3)),
    );
    assert_eq!(
        stage
            .mirrors
            .accepted_authors
            .get(&("kept".to_string(), direct_identity.clone())),
        Some(&"direct-author".to_string()),
    );
    assert_eq!(
        stage
            .mirrors
            .accepted_authors
            .get(&("kept".to_string(), relayed_identity.clone())),
        Some(&"original-author-through-relay".to_string()),
    );
    let staged_lineages = stage
        .persistence
        .lineage_values
        .iter()
        .map(|(_, bytes)| {
            RedbPersistence::decode_config_value::<DurableRowLineageSidecar>(bytes).unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(staged_lineages.len(), 2);
    assert!(
        staged_lineages
            .iter()
            .any(|sidecar| sidecar.author_node_id == "direct-author")
    );
    assert!(
        staged_lineages
            .iter()
            .any(|sidecar| sidecar.author_node_id == "original-author-through-relay")
    );
    let staged_authors = stage
        .persistence
        .accepted_author_values
        .iter()
        .map(|(_, bytes)| RedbPersistence::decode_config_value::<String>(bytes).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        staged_authors,
        vec!["direct-author", "original-author-through-relay"]
    );
    let derived = db.scoped_with_constraints(None, None, None);
    derived
        .register_received_schema_stage(Lsn(9), stage)
        .expect("register received-schema stage")
        .leave_registered();
    let stage = db
        .take_received_schema_stage(Lsn(9))
        .expect("derived handle shares the commit-LSN stage");
    derived.publish_received_schema_stage(stage);

    assert!(db.table_meta("kept").is_some());
    assert!(db.table_meta("dropped").is_none());
    assert!(db.table_meta("events").is_none());
    assert_eq!(db.relational_store.tables.read()["kept"].len(), 3);
    assert_eq!(db.graph_store.forward_adj.read().len(), 2);
    let vector_fixture_reader_11 = db
        .declared_reader_memory()
        .expect("admit the vector fixture reader");
    let mut vector_fixture_copy_11 =
        contextdb_engine::vector_read_memory::admission(vector_fixture_reader_11.clone());

    assert_eq!(
        db.vector_store
            .all_entries(&mut vector_fixture_copy_11)
            .expect("read or publish the admitted vector fixture")
            .len(),
        3
    );
    assert!(db.list_triggers().is_empty());
    assert_eq!(
        db.in_memory_table_generations.lock().get("dropped"),
        Some(&4)
    );
    assert_eq!(
        db.in_memory_ddl_generations.lock().get(&(Lsn(9), 0)),
        Some(&(Some("kept".to_string()), Some(4)))
    );
    assert!(db.sync_tombstone_arrivals.read().is_empty());
    assert_eq!(
        db.accepted_sync_row_authors
            .read()
            .get(&("kept".to_string(), direct_identity)),
        Some(&"direct-author".to_string()),
    );
    assert_eq!(
        db.accepted_sync_row_authors
            .read()
            .get(&("kept".to_string(), relayed_identity)),
        Some(&"original-author-through-relay".to_string()),
    );
}
fn received_single_ddl(ddl: &DdlChange, generation: u64) -> crate::protocol::ReceivedDdlContext {
    crate::protocol::ReceivedDdlContext {
        tenant_id: TenantId::from("tenant-a"),
        source_node_id: "relay-node".to_string(),
        source_incarnation: Incarnation(7),
        entries: vec![crate::protocol::ReceivedDdlEntry {
            source_ddl_lsn: Lsn(41),
            ordinal: 0,
            table: Database::ddl_affected_table(ddl).map(str::to_string),
            table_generation: Some(generation),
            digest: vec![1],
            author: None,
        }],
    }
}

/// A SIGNED unit's trigger audit counts every trigger the unit declares,
/// not merely its final trigger set. This matches the direct door's
/// `sync_pull_trigger_audit_projection`, so a row in the same source
/// transaction as `CreateTrigger ... DropTrigger` still fires and is
/// audited in author order.
#[test]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn a_signed_units_trigger_audit_counts_every_trigger_it_declares() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, body TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    // No trigger exists yet, so this latches `trigger.ready` permanently:
    // a trigger this unit declares later needs no local callback.
    db.complete_initialization().unwrap();

    let row_id = uuid::Uuid::from_u128(1);
    let ddl = vec![
        DdlChange::CreateTrigger {
            name: "tr".to_string(),
            table: "t".to_string(),
            on_events: vec!["INSERT".to_string()],
        },
        DdlChange::DropTrigger {
            name: "tr".to_string(),
        },
    ];
    let row_key = NaturalKey::single("id".to_string(), Value::Uuid(row_id));
    let changes = ChangeSet {
        ddl: ddl.clone(),
        ddl_lsn: vec![Lsn(7), Lsn(7)],
        rows: vec![RowChange {
            table: "t".to_string(),
            natural_key: row_key.clone(),
            values: HashMap::from([
                ("id".to_string(), Value::Uuid(row_id)),
                (
                    "body".to_string(),
                    Value::Text("declared-and-dropped".to_string()),
                ),
            ]),
            deleted: false,
            lsn: Lsn(7),
            created_at: None,
        }],
        ..ChangeSet::default()
    };
    let row_lineage = vec![("t".to_string(), row_key, Lsn(7), lineage("origin", 501, 1))];
    let received = crate::protocol::ReceivedDdlContext {
        tenant_id: TenantId::from("tenant-a"),
        source_node_id: "relay-node".to_string(),
        source_incarnation: Incarnation(7),
        entries: vec![
            crate::protocol::ReceivedDdlEntry {
                source_ddl_lsn: Lsn(7),
                ordinal: 0,
                table: Some("t".to_string()),
                table_generation: Some(1),
                digest: vec![1],
                author: None,
            },
            crate::protocol::ReceivedDdlEntry {
                source_ddl_lsn: Lsn(7),
                ordinal: 1,
                table: None,
                table_generation: None,
                digest: vec![2],
                author: None,
            },
        ],
    };
    let received_schema_read_memory_12 = db
        .declared_reader_memory()
        .expect("admit the received-schema operation reader");
    let mut received_schema_copy_owner_12 =
        contextdb_engine::vector_read_memory::admission(received_schema_read_memory_12.clone());

    db.commit_received_schema_stage_for_test(
        &changes,
        &received,
        &row_lineage,
        &HashMap::new(),
        false,
        &mut received_schema_copy_owner_12,
        received_schema_read_memory_12,
    )
    .unwrap();

    let commit_lsn = db
        .changes_since(Lsn(0))
        .expect("read the declared changes")
        .rows
        .iter()
        .map(|row| row.lsn)
        .max()
        .expect("the received row committed");
    let audit = db
        .trigger_audit_history(TriggerAuditFilter {
            trigger_name: Some("tr".to_string()),
            status: Some(TriggerAuditStatusFilter::Fired),
        })
        .unwrap();
    assert_eq!(
        audit.len(),
        1,
        "the declared-and-dropped trigger fires exactly once: {audit:?}"
    );
    assert_eq!(
        audit[0].firing_lsn, commit_lsn,
        "the fire is recorded at the receiver's own commit LSN"
    );
    assert!(
        db.list_triggers().is_empty(),
        "the trigger is dropped in the same unit it was declared in"
    );
    assert_eq!(
        db.execute("SELECT id FROM t", &HashMap::new())
            .unwrap()
            .rows(),
        vec![vec![Value::Uuid(row_id)]],
        "the row itself is still installed"
    );

    // Control: the same shape of unit, but with no trigger DDL at all and
    // `tr` already declared (with a registered callback) on the
    // receiver, still fires exactly once -- proving the count above came
    // from the audit, not from an accident of this fixture.
    let control = Database::open_memory();
    control
        .execute(
            "CREATE TABLE t (id UUID PRIMARY KEY, body TEXT)",
            &HashMap::new(),
        )
        .unwrap();
    control
        .execute("CREATE TRIGGER tr ON t WHEN INSERT", &HashMap::new())
        .unwrap();
    control
        .register_trigger_callback("tr", |_db, _ctx| Ok(()))
        .unwrap();
    control.complete_initialization().unwrap();
    let control_row_id = uuid::Uuid::from_u128(2);
    let control_key = NaturalKey::single("id".to_string(), Value::Uuid(control_row_id));
    let control_changes = ChangeSet {
        rows: vec![RowChange {
            table: "t".to_string(),
            natural_key: control_key.clone(),
            values: HashMap::from([
                ("id".to_string(), Value::Uuid(control_row_id)),
                (
                    "body".to_string(),
                    Value::Text("no-trigger-ddl".to_string()),
                ),
            ]),
            deleted: false,
            lsn: Lsn(7),
            created_at: None,
        }],
        ..ChangeSet::default()
    };
    let control_received = crate::protocol::ReceivedDdlContext {
        tenant_id: TenantId::from("tenant-a"),
        source_node_id: "relay-node".to_string(),
        source_incarnation: Incarnation(7),
        entries: Vec::new(),
    };
    let control_lineage = vec![(
        "t".to_string(),
        control_key,
        Lsn(7),
        lineage("origin", 502, 1),
    )];
    let received_schema_read_memory_13 = control
        .declared_reader_memory()
        .expect("admit the received-schema operation reader");
    let mut received_schema_copy_owner_13 =
        contextdb_engine::vector_read_memory::admission(received_schema_read_memory_13.clone());

    control
        .commit_received_schema_stage_for_test(
            &control_changes,
            &control_received,
            &control_lineage,
            &HashMap::new(),
            false,
            &mut received_schema_copy_owner_13,
            received_schema_read_memory_13,
        )
        .unwrap();
    let control_audit = control
        .trigger_audit_history(TriggerAuditFilter {
            trigger_name: Some("tr".to_string()),
            status: Some(TriggerAuditStatusFilter::Fired),
        })
        .unwrap();
    assert_eq!(
        control_audit.len(),
        1,
        "the control unit, with no trigger DDL, still fires the pre-declared trigger exactly \
             once: {control_audit:?}"
    );
}

/// Including-sync rows in a SIGNED unit copy only the affected tables.
/// A transactional callback that reaches another table discovers and adds
/// it lazily; the many unrelated tables the receiver also holds are never
/// staged.
#[test]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn a_signed_units_including_sync_rows_copy_only_the_tables_the_callback_touches() {
    let db = Database::open_memory();
    for ordinal in 0..100 {
        db.execute(
            &format!("CREATE TABLE u{ordinal} (id INTEGER PRIMARY KEY)"),
            &HashMap::new(),
        )
        .unwrap();
    }
    db.execute("CREATE TABLE s (id INTEGER PRIMARY KEY)", &HashMap::new())
        .unwrap();
    db.execute(
        "CREATE TABLE d (id INTEGER PRIMARY KEY, source_id INTEGER)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE TRIGGER sync_s ON s WHEN INSERT INCLUDING SYNC",
        &HashMap::new(),
    )
    .unwrap();
    db.register_trigger_callback("sync_s", |handle, context| {
        let id = context.row_values["id"].clone();
        handle.execute_in_tx(
            context.tx,
            "INSERT INTO d (id, source_id) VALUES ($id, $source_id)",
            &HashMap::from([
                ("id".to_string(), id.clone()),
                ("source_id".to_string(), id),
            ]),
        )?;
        Ok(())
    })
    .unwrap();
    db.complete_initialization().unwrap();

    let ddl = vec![DdlChange::CreateIndex {
        table: "s".to_string(),
        name: "s_id_idx".to_string(),
        columns: vec![("id".to_string(), SortDirection::Asc)],
    }];
    let row_key = NaturalKey::single("id".to_string(), Value::Int64(1));
    let changes = ChangeSet {
        ddl: ddl.clone(),
        ddl_lsn: vec![Lsn(41)],
        rows: vec![RowChange {
            table: "s".to_string(),
            natural_key: row_key.clone(),
            values: HashMap::from([("id".to_string(), Value::Int64(1))]),
            deleted: false,
            lsn: Lsn(41),
            created_at: None,
        }],
        ..ChangeSet::default()
    };
    let received = received_single_ddl(&ddl[0], 1);
    let row_lineage = vec![("s".to_string(), row_key, Lsn(41), lineage("origin", 501, 1))];

    let trace = ReceivedSchemaApplyObserverForTest::new();
    let scope = db.observe_received_schema_applies_for_test(&trace);
    let received_schema_read_memory_14 = db
        .declared_reader_memory()
        .expect("admit the received-schema operation reader");
    let mut received_schema_copy_owner_14 =
        contextdb_engine::vector_read_memory::admission(received_schema_read_memory_14.clone());

    db.commit_received_schema_stage_for_test(
        &changes,
        &received,
        &row_lineage,
        &HashMap::new(),
        false,
        &mut received_schema_copy_owner_14,
        received_schema_read_memory_14,
    )
    .unwrap();
    drop(scope);
    let observation = trace
        .observation()
        .expect("the real signed stage attached its observer");
    assert_eq!(
        observation.staged_table_names,
        BTreeSet::from(["s".to_string(), "d".to_string()]),
        "the callback's write to d is discovered and staged; none of the 100 unrelated \
             tables is: {observation:?}"
    );

    assert_eq!(
        db.execute("SELECT id FROM s", &HashMap::new())
            .unwrap()
            .rows(),
        vec![vec![Value::Int64(1)]]
    );
    assert_eq!(
        db.execute("SELECT source_id FROM d", &HashMap::new())
            .unwrap()
            .rows(),
        vec![vec![Value::Int64(1)]],
        "the including-sync callback's write actually committed with the received row"
    );
    let log = db.changes_since(Lsn(0)).expect("read the declared changes");
    let commit_lsn = log
        .rows
        .iter()
        .map(|row| row.lsn)
        .max()
        .expect("both rows committed");
    assert!(
        log.rows.iter().all(|row| row.lsn == commit_lsn),
        "s's row and the callback's write to d land at the same received commit LSN: {:?}",
        log.rows
    );

    // An identical second delivery of the same unit finds nothing left to
    // discover and copies nothing.
    let trace2 = ReceivedSchemaApplyObserverForTest::new();
    let scope2 = db.observe_received_schema_applies_for_test(&trace2);
    let received_schema_read_memory_15 = db
        .declared_reader_memory()
        .expect("admit the received-schema operation reader");
    let mut received_schema_copy_owner_15 =
        contextdb_engine::vector_read_memory::admission(received_schema_read_memory_15.clone());

    db.commit_received_schema_stage_for_test(
        &changes,
        &received,
        &row_lineage,
        &HashMap::new(),
        false,
        &mut received_schema_copy_owner_15,
        received_schema_read_memory_15,
    )
    .unwrap();
    drop(scope2);
    let observation2 = trace2
        .observation()
        .expect("the retried stage attached its observer too");
    assert_eq!(
        observation2.staged_table_images_copied, 0,
        "an identical retried delivery stages nothing further: {observation2:?}"
    );
}

fn receipt(source_lsn: u64) -> SyncApplyReceipt {
    SyncApplyReceipt {
        tenant_id: TenantId::from("tenant-a"),
        node_id: "relay-node".to_string(),
        incarnation: Incarnation(9),
        source_lsn: Lsn(source_lsn),
        dependency_complete: false,
        hub_contact_ms: None,
    }
}

type ReceivedSchemaFixture = (
    ChangeSet,
    crate::protocol::ReceivedDdlContext,
    Vec<(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)>,
);

fn received_schema_create_with_row() -> ReceivedSchemaFixture {
    let ddl = create_table();
    let key = NaturalKey::single("id".to_string(), Value::Int64(1));
    (
        ChangeSet {
            ddl: vec![ddl.clone()],
            ddl_lsn: vec![Lsn(41)],
            rows: vec![RowChange {
                table: "events".to_string(),
                natural_key: key.clone(),
                values: HashMap::from([("id".to_string(), Value::Int64(1))]),
                deleted: false,
                lsn: Lsn(42),
                created_at: None,
            }],
            ..ChangeSet::default()
        },
        received_single_ddl(&ddl, 3),
        vec![(
            "events".to_string(),
            key,
            Lsn(42),
            lineage("origin", 501, 3),
        )],
    )
}

#[test]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn received_schema_stage_persists_and_publishes_only_after_redb_commit() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("received-schema-stage.redb");
    let db = Database::open(&path).unwrap();
    let (changes, received, lineages) = received_schema_create_with_row();
    let received_schema_read_memory_16 = db
        .declared_reader_memory()
        .expect("admit the received-schema operation reader");
    let mut received_schema_copy_owner_16 =
        contextdb_engine::vector_read_memory::admission(received_schema_read_memory_16.clone());

    db.commit_received_schema_stage_for_test(
        &changes,
        &received,
        &lineages,
        &HashMap::new(),
        false,
        &mut received_schema_copy_owner_16,
        received_schema_read_memory_16,
    )
    .unwrap();

    assert!(db.received_schema_stages.lock().is_empty());
    assert!(db.table_meta("events").is_some());
    assert_eq!(
        db.execute("SELECT * FROM events", &HashMap::new())
            .unwrap()
            .rows()
            .len(),
        1
    );
    let immediate_changes = db.changes_since(Lsn(0)).expect("read the declared changes");
    assert_eq!(immediate_changes.rows.len(), 1);
    let durable_coordinates = |database: &Database, table: &str| {
        let mut coordinates = database
            .change_log_since(Lsn(0))
            .expect("read durable table history")
            .into_iter()
            .filter_map(|entry| match entry {
                ChangeLogEntry::RowInsert {
                    table: name,
                    row_id,
                    lsn,
                }
                | ChangeLogEntry::RowDelete {
                    table: name,
                    row_id,
                    lsn,
                    ..
                } if name == table => Some((lsn, row_id)),
                _ => None,
            })
            .collect::<Vec<_>>();
        coordinates.sort();
        coordinates
    };
    let table_index = durable_coordinates(&db, "events");
    assert_eq!(table_index.len(), 1);
    let staged_lsn = table_index[0].0;
    assert_eq!(
        immediate_changes
            .rows
            .iter()
            .filter(|row| row.lsn == staged_lsn)
            .count(),
        1
    );
    let mut received_occurrences = Vec::new();
    db.visit_received_ddl_arrivals(
        |occurrence, _| {
            received_occurrences.push(occurrence);
            Ok(())
        },
        db.declared_reader_memory()
            .expect("admit the DDL arrival reader"),
    )
    .expect("visit durable received schema arrivals");
    assert_eq!(received_occurrences.len(), 1);
    let (received_ddl_lsn, received_ordinal) = received_occurrences[0];
    assert_eq!(received_ordinal, 0);
    assert_eq!(immediate_changes.ddl_lsn, vec![received_ddl_lsn]);
    let fixture_operation_reader_32 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");
    let mut fixture_operation_copy_32 =
        contextdb_engine::vector_read_memory::admission(fixture_operation_reader_32.clone());

    let (pending_before_close, source_before_close) = db
        .changes_since_base(
            Lsn(received_ddl_lsn.0.saturating_sub(1)),
            &mut fixture_operation_copy_32,
            fixture_operation_reader_32.clone(),
        )
        .expect("materialize received DDL changes before close");
    let fixture_operation_reader_33 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    assert!(
        db.filter_outbound_received_ddl(
            pending_before_close,
            &source_before_close,
            None,
            fixture_operation_reader_33.clone()
        )
        .unwrap()
        .ddl
        .is_empty()
    );
    db.close().unwrap();
    drop(db);

    let reopened = Database::open(&path).unwrap();
    assert!(reopened.table_meta("events").is_some());
    assert_eq!(
        reopened
            .execute("SELECT * FROM events", &HashMap::new())
            .unwrap()
            .rows()
            .len(),
        1
    );
    let reopened_changes = reopened
        .changes_since(Lsn(0))
        .expect("read the declared changes");
    assert_eq!(reopened_changes.rows, immediate_changes.rows);
    assert_eq!(reopened_changes.ddl, immediate_changes.ddl);
    assert_eq!(reopened_changes.ddl_lsn, immediate_changes.ddl_lsn);
    assert_eq!(durable_coordinates(&reopened, "events"), table_index);
    assert_eq!(
        reopened_changes
            .rows
            .iter()
            .filter(|row| row.lsn == staged_lsn)
            .count(),
        1
    );
    let fixture_operation_reader_2 = reopened
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    assert!(
        reopened
            .with_received_ddl_arrival(
                (received_ddl_lsn, 0),
                |arrival| Ok(arrival.is_some()),
                fixture_operation_reader_2.clone()
            )
            .expect("read the durable received occurrence after reopen")
    );
    let fixture_operation_reader_34 = reopened
        .declared_reader_memory()
        .expect("admit the actual fixture reader");
    let mut fixture_operation_copy_34 =
        contextdb_engine::vector_read_memory::admission(fixture_operation_reader_34.clone());

    let (pending_after_reopen, source_after_reopen) = reopened
        .changes_since_base(
            Lsn(received_ddl_lsn.0.saturating_sub(1)),
            &mut fixture_operation_copy_34,
            fixture_operation_reader_34.clone(),
        )
        .expect("materialize received DDL changes after reopen");
    let fixture_operation_reader_35 = reopened
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    assert!(
        reopened
            .filter_outbound_received_ddl(
                pending_after_reopen,
                &source_after_reopen,
                None,
                fixture_operation_reader_35.clone()
            )
            .unwrap()
            .ddl
            .is_empty()
    );
}

fn received_schema_create_vector_table_with_row_and_vector() -> ReceivedSchemaFixture {
    let ddl = DdlChange::CreateTable {
        name: "vector_events".to_string(),
        columns: vec![
            ("id".to_string(), "INTEGER PRIMARY KEY".to_string()),
            ("embedding".to_string(), "VECTOR(2)".to_string()),
        ],
        constraints: Vec::new(),
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    };
    let key = NaturalKey::single("id".to_string(), Value::Int64(1));
    (
        ChangeSet {
            ddl: vec![ddl.clone()],
            ddl_lsn: vec![Lsn(41)],
            rows: vec![RowChange {
                table: "vector_events".to_string(),
                natural_key: key.clone(),
                values: HashMap::from([
                    ("id".to_string(), Value::Int64(1)),
                    ("embedding".to_string(), Value::Vector(vec![1.0, 0.0])),
                ]),
                deleted: false,
                lsn: Lsn(42),
                created_at: None,
            }],
            vectors: vec![VectorChange {
                index: VectorIndexRef::new("vector_events", "embedding"),
                row_id: RowId(1),
                vector: vec![1.0, 0.0],
                lsn: Lsn(42),
            }],
            ..ChangeSet::default()
        },
        received_single_ddl(&ddl, 3),
        vec![(
            "vector_events".to_string(),
            key,
            Lsn(42),
            lineage("origin", 501, 3),
        )],
    )
}

/// The decisive proof for W6: an AUTHENTICATED unit that declares a
/// table with a vector column and carries that table's rows and vectors
/// in the SAME delivery, through the shipped commit path
/// (`commit_received_schema_stage`, not the lower-level prepare/publish
/// pair that skips the transaction manager entirely).
#[test]
fn an_authenticated_unit_that_declares_a_vector_table_installs_its_vectors_in_the_same_delivery() {
    let db = Database::open_memory();
    let (changes, received, lineages) = received_schema_create_vector_table_with_row_and_vector();
    let received_schema_read_memory_17 = db
        .declared_reader_memory()
        .expect("admit the received-schema operation reader");
    let mut received_schema_copy_owner_17 =
        contextdb_engine::vector_read_memory::admission(received_schema_read_memory_17.clone());

    db.commit_received_schema_stage_for_test(
        &changes,
        &received,
        &lineages,
        &HashMap::new(),
        false,
     &mut received_schema_copy_owner_17, received_schema_read_memory_17)
    .expect(
        "an authenticated unit declaring its own vector table must install schema and vectors together",
    );

    assert!(db.table_meta("vector_events").is_some());
    let mut params = HashMap::new();
    params.insert("q".to_string(), Value::Vector(vec![1.0, 0.0]));
    let nearest = db
        .execute(
            "SELECT id FROM vector_events ORDER BY embedding <=> $q LIMIT 1",
            &params,
        )
        .expect("the declared vector index must be searchable after installing whole");
    assert_eq!(nearest.rows().len(), 1);
}

/// A mixed schema + rows + vectors unit installs whole on
/// a file-backed hub, at ONE commit LSN, with a durable per-unit receipt.
/// The author declares a vector table and writes rows into it; the unit
/// is pushed exactly as sync sends it. The proof reads the receipt back
/// from the hub's durable store after a restart -- the whole-unit schema
/// record naming the author's identity, the exact request's stored
/// outcome, and the author's delivery frontier -- rather than trusting
/// the apply call's own answer, and it checks that the schema, every row
/// and every vector carry the one LSN the hub advanced by.
#[test]
fn a_mixed_schema_rows_and_vectors_unit_installs_whole_with_one_lsn_and_a_receipt() {
    let root = tempfile::TempDir::new().expect("scratch peer directory");
    let author = JourneyPeer::open(root.path(), "mixed-author");
    let hub = JourneyPeer::open(root.path(), "mixed-hub");
    author
        .db
        .execute(
            "CREATE TABLE vector_events (id UUID PRIMARY KEY, body TEXT, embedding VECTOR(2))",
            &HashMap::new(),
        )
        .expect("the author declares a vector table");
    let near = uuid::Uuid::new_v4();
    let far = uuid::Uuid::new_v4();
    for (id, body, vector) in [(near, "near", vec![1.0, 0.0]), (far, "far", vec![0.0, 1.0])] {
        author
            .db
            .execute(
                "INSERT INTO vector_events (id, body, embedding) VALUES ($id, $body, $v)",
                &HashMap::from([
                    ("id".to_string(), Value::Uuid(id)),
                    ("body".to_string(), Value::Text(body.to_string())),
                    ("v".to_string(), Value::Vector(vector)),
                ]),
            )
            .expect("the author writes a row with its vector");
    }
    let served = served_by(&author, Lsn(0));
    assert_eq!(
        (served.ddl.len(), served.rows.len(), served.vectors.len()),
        (1, 2, 2),
        "one unit carrying schema, rows and vectors"
    );

    let lsn_before = hub.db.current_lsn();
    let pushed = pushed_to(&hub, &author, served.clone());
    assert!(pushed.result.conflicts.is_empty(), "{:?}", pushed.result);
    assert_eq!(pushed.result.applied_rows, 2, "both rows applied");
    let unit_lsn = pushed.result.new_lsn;
    assert_eq!(
        hub.db.current_lsn(),
        unit_lsn,
        "the hub's LSN stops at the unit's commit"
    );
    assert_eq!(
        unit_lsn,
        Lsn(lsn_before.0 + 1),
        "schema, rows and vectors consumed exactly one commit LSN"
    );
    let installed = hub
        .db
        .changes_since(lsn_before)
        .expect("read the declared changes");
    assert_eq!(
        (
            installed.ddl.len(),
            installed.rows.len(),
            installed.vectors.len()
        ),
        (1, 2, 2),
        "the hub holds the whole unit"
    );
    assert!(
        installed.ddl_lsn.iter().all(|lsn| *lsn == unit_lsn)
            && installed.rows.iter().all(|row| row.lsn == unit_lsn)
            && installed
                .vectors
                .iter()
                .all(|vector| vector.lsn == unit_lsn),
        "every DDL record, row and vector carries the one unit LSN {unit_lsn:?}: {:?} / {:?} \
             / {:?}",
        installed.ddl_lsn,
        installed.rows.iter().map(|row| row.lsn).collect::<Vec<_>>(),
        installed
            .vectors
            .iter()
            .map(|vector| vector.lsn)
            .collect::<Vec<_>>()
    );

    let hub = hub.reopen();
    // The receipt, read back from the durable store rather than from the
    // apply call: the whole-unit schema record binds the author's node,
    // incarnation, source LSN, ordinal and digest ...
    let received = pushed.received.as_ref().expect("the unit carries schema");
    let manifest = assert_durable_manifest_matches(&hub, received, "after restart");
    assert_eq!(received.source_node_id, author.node_id());
    assert_eq!(received.source_incarnation, author.incarnation());
    assert_eq!(
        received
            .entries
            .iter()
            .map(|entry| (entry.source_ddl_lsn, entry.ordinal, entry.digest.clone()))
            .collect::<Vec<_>>(),
        served
            .ddl_provenance
            .iter()
            .map(|entry| (entry.source_ddl_lsn, entry.ordinal, entry.digest.clone()))
            .collect::<Vec<_>>(),
        "the recorded identity is the author's attested identity"
    );
    assert_eq!(
        (manifest.result.new_lsn, manifest.result.applied_rows),
        (unit_lsn, 2),
        "the durable record stores the unit's one commit LSN and its outcome"
    );
    // ... the exact request's outcome is stored under the author's
    // incarnation and this request's digest ...
    let outcome = hub
        .persisted::<DurableSyncPushOutcome>(&pushed.outcome_key.storage_key)
        .expect("the hub durably stored this request's outcome");
    assert_eq!(outcome.incarnation, author.incarnation());
    assert_eq!(outcome.request_digest, pushed.outcome_key.request_digest);
    assert_eq!(
        (outcome.result.new_lsn, outcome.result.applied_rows),
        (unit_lsn, 2),
        "the stored outcome names the unit's one commit LSN"
    );
    // ... and the author's delivery frontier advanced to exactly this unit.
    assert_eq!(
        hub.db
            .persisted_sync_applied_push_watermark_for_node_incarnation(
                &pushed.receipt.tenant_id,
                &author.node_id(),
                author.incarnation(),
            )
            .expect("read the durable frontier"),
        Some(pushed.receipt.source_lsn),
        "the durable frontier is exactly the pushed unit's source frontier"
    );
    assert_eq!(pushed.receipt.source_lsn, served_max_lsn(&served));

    assert!(
        hub.db.table_meta("vector_events").is_some(),
        "the declaration survives restart"
    );
    let rows = hub
        .db
        .execute("SELECT id FROM vector_events", &HashMap::new())
        .expect("the rows answer an ordinary SELECT after restart");
    assert_eq!(rows.rows().len(), 2, "both rows installed with the schema");
    let nearest = hub
        .db
        .execute(
            "SELECT id FROM vector_events ORDER BY embedding <=> $q LIMIT 1",
            &HashMap::from([("q".to_string(), Value::Vector(vec![1.0, 0.0]))]),
        )
        .expect("the vectors are searchable after restart");
    assert_eq!(nearest.rows(), vec![vec![Value::Uuid(near)]]);
    hub.db.close().expect("close the hub");
    author.db.close().expect("close the author");
}

fn served_max_lsn(wire: &crate::protocol::WireChangeSet) -> Lsn {
    wire.ddl_lsn
        .iter()
        .copied()
        .chain(wire.rows.iter().map(|row| row.lsn))
        .chain(wire.vectors.iter().map(|vector| vector.lsn))
        .max()
        .expect("a nonempty unit")
}

/// An unattested unit's CREATE mints a table generation. A later
/// unattested delivery to that table uses the generation to apply.
#[test]
fn an_unattested_units_table_generation_survives_to_a_second_delivery() {
    let db = Database::open_memory();
    let policies = ConflictPolicies::uniform(ConflictPolicy::LatestWins);
    let table = "unattested_vector_events";
    let column = "embedding";
    let create = ChangeSet {
        ddl: vec![DdlChange::CreateTable {
            name: table.to_string(),
            columns: vec![
                ("id".to_string(), "INTEGER PRIMARY KEY".to_string()),
                (column.to_string(), "VECTOR(2)".to_string()),
            ],
            constraints: Vec::new(),
            foreign_keys: Vec::new(),
            composite_foreign_keys: Vec::new(),
            composite_unique: Vec::new(),
        }],
        ddl_lsn: vec![Lsn(1)],
        rows: vec![RowChange {
            table: table.to_string(),
            natural_key: NaturalKey::single("id".to_string(), Value::Int64(1)),
            values: HashMap::from([
                ("id".to_string(), Value::Int64(1)),
                (column.to_string(), Value::Vector(vec![1.0, 0.0])),
            ]),
            deleted: false,
            lsn: Lsn(1),
            created_at: None,
        }],
        vectors: vec![VectorChange {
            index: VectorIndexRef::new(table, column),
            row_id: RowId(1),
            vector: vec![1.0, 0.0],
            lsn: Lsn(1),
        }],
        ..ChangeSet::default()
    };
    db.apply_changes(create, &policies)
        .expect("an unattested unit installs its own table and vectors whole");

    let generation_after_create = db.in_memory_table_generations.lock().get(table).copied();
    assert_eq!(
        generation_after_create,
        Some(1),
        "an unattested CREATE must mint a real generation, exactly like a local CREATE would \
             (next_table_generation_for_create), not leave the table's generation absent"
    );

    let second_delivery = ChangeSet {
        rows: vec![RowChange {
            table: table.to_string(),
            natural_key: NaturalKey::single("id".to_string(), Value::Int64(2)),
            values: HashMap::from([
                ("id".to_string(), Value::Int64(2)),
                (column.to_string(), Value::Vector(vec![0.0, 1.0])),
            ]),
            deleted: false,
            lsn: Lsn(2),
            created_at: None,
        }],
        vectors: vec![VectorChange {
            index: VectorIndexRef::new(table, column),
            row_id: RowId(2),
            vector: vec![0.0, 1.0],
            lsn: Lsn(2),
        }],
        ..ChangeSet::default()
    };
    db.apply_changes(second_delivery, &policies).expect(
        "a second unattested delivery to a table an earlier unattested unit created must \
             still apply -- the table's generation must not have gone missing",
    );
    assert_eq!(
        db.in_memory_table_generations.lock().get(table).copied(),
        Some(1),
        "the second delivery carries no DDL, so the generation stays exactly what the CREATE minted"
    );
}

fn unattested_row(table: &str, id: i64, lsn: Lsn) -> RowChange {
    RowChange {
        table: table.to_string(),
        natural_key: NaturalKey::single("id".to_string(), Value::Int64(id)),
        values: HashMap::from([("id".to_string(), Value::Int64(id))]),
        deleted: false,
        lsn,
        created_at: None,
    }
}

fn unattested_create(table: &str) -> DdlChange {
    DdlChange::CreateTable {
        name: table.to_string(),
        columns: vec![("id".to_string(), "INTEGER PRIMARY KEY".to_string())],
        constraints: Vec::new(),
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    }
}

fn forwarded_copy_holds(receiver: &Database, table: &str, rows: usize) {
    let forwarded = Database::open_memory();
    forwarded
        .apply_changes(
            receiver
                .changes_since(Lsn(0))
                .expect("read the declared changes"),
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .expect("what the receiver took delivery of forwards to a further peer");
    assert!(forwarded.table_meta(table).is_some());
    let receiver_snapshot = receiver.snapshot();
    let receiver_ids = receiver
        .scan(table, receiver_snapshot)
        .expect("the receiver's own table is readable")
        .into_iter()
        .map(|row| row.values.get("id").cloned())
        .collect::<Vec<_>>();
    let forwarded_snapshot = forwarded.snapshot();
    let forwarded_ids = forwarded
        .scan(table, forwarded_snapshot)
        .expect("the forwarded table is readable")
        .into_iter()
        .map(|row| row.values.get("id").cloned())
        .collect::<Vec<_>>();
    assert_eq!(
        forwarded_ids.len(),
        rows,
        "the further peer holds every row the receiver holds"
    );
    // A row count match alone cannot tell a genuine second hop from one
    // that silently swapped in a different but equally numerous set of
    // rows. Each row's identity -- its own natural key, the only identity
    // this unattested fixture carries -- must be the exact same set on
    // both sides. Signed author identity of every forwarded row and schema
    // record is guarded over an
    // authenticated author -> hub -> edge journey by
    // `a_second_peer_holds_the_authors_signed_identity_not_the_forwarding_hubs`.
    let mut receiver_sorted = receiver_ids.clone();
    receiver_sorted.sort_by_key(|value| format!("{value:?}"));
    let mut forwarded_sorted = forwarded_ids;
    forwarded_sorted.sort_by_key(|value| format!("{value:?}"));
    assert_eq!(
        forwarded_sorted, receiver_sorted,
        "the further peer holds the exact same rows the receiver holds, by identity, not \
             merely the same count"
    );
}

/// Second-peer forwarding preserves author identity.
///
/// An author declares a table (create then alter, one source transaction)
/// and writes rows; it pushes the unit to a hub through the authenticated
/// door; an edge then pulls what the hub holds. Every row the edge stores
/// must still name the AUTHOR as its signed creator -- node id,
/// incarnation, mutation position, generation, lineage root and the
/// author's own signature -- never the hub that forwarded it, and it must
/// survive the edge's restart. Every schema record the edge stores must be
/// the author's statement in the author's order, ordinal and table
/// generation, bound to a digest that recomputes from the author's own
/// statement. Every schema record must also name the AUTHOR as its
/// writer -- the author's node id, incarnation, source LSN, ordinal, table
/// generation and attested digest -- before and after the edge restarts,
/// and a fourth peer that takes delivery from the edge must hold that same
/// author identity. (The delivery bookkeeping -- the marker's node,
/// incarnation and LSN -- still names the peer that delivered it; that is
/// asserted too, so a change to it is seen.)
#[test]
#[expect(
    clippy::into_iter_on_ref,
    reason = "The fixture iterates the borrowed query-result rows without consuming the retained answer."
)]
fn a_second_peer_holds_the_authors_signed_identity_not_the_forwarding_hubs() {
    let root = tempfile::TempDir::new().expect("scratch peer directory");
    let author = JourneyPeer::open(root.path(), "forward-author");
    let hub = JourneyPeer::open(root.path(), "forward-hub");
    let edge = JourneyPeer::open(root.path(), "forward-edge");
    let tenant = TenantId::from(JOURNEY_TENANT);

    let tx = author.db.begin().expect("the author opens one transaction");
    for statement in [
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT)",
        "ALTER TABLE notes ADD COLUMN tag TEXT",
    ] {
        author
            .db
            .execute_in_tx(tx, statement, &HashMap::new())
            .unwrap_or_else(|error| panic!("the author runs `{statement}`: {error:?}"));
    }
    author.db.commit(tx).expect("the author commits its schema");
    // The author's client pushes one source transaction per request: the
    // schema first, then the rows.
    let from_author = served_by(&author, Lsn(0));
    assert_eq!((from_author.ddl.len(), from_author.rows.len()), (2, 0));
    let pushed = pushed_to(&hub, &author, from_author.clone());
    assert!(pushed.result.conflicts.is_empty(), "{:?}", pushed.result);

    let schema_lsn = author.db.current_lsn();
    let ids = [uuid::Uuid::new_v4(), uuid::Uuid::new_v4()];
    let tx = author.db.begin().expect("the author opens one transaction");
    for (index, id) in ids.iter().enumerate() {
        author
            .db
            .execute_in_tx(
                tx,
                "INSERT INTO notes (id, body, tag) VALUES ($id, $body, 't')",
                &HashMap::from([
                    ("id".to_string(), Value::Uuid(*id)),
                    ("body".to_string(), Value::Text(format!("note {index}"))),
                ]),
            )
            .expect("the author writes a row");
    }
    author.db.commit(tx).expect("the author commits its rows");
    let rows_from_author = served_by(&author, schema_lsn);
    assert_eq!(
        (rows_from_author.ddl.len(), rows_from_author.rows.len()),
        (0, 2)
    );
    let authored_lineages = crate::protocol::wire_row_lineages(&rows_from_author);
    assert_eq!(
        authored_lineages.len(),
        2,
        "each row carries a signed lineage"
    );
    for (_, _, _, lineage) in &authored_lineages {
        assert_eq!(lineage.author_node_id, author.node_id());
        assert_eq!(lineage.author_database_incarnation, author.incarnation());
    }
    let pushed = pushed_to(&hub, &author, rows_from_author);
    assert!(pushed.result.conflicts.is_empty(), "{:?}", pushed.result);
    assert_eq!(pushed.result.applied_rows, 2);

    let from_hub = served_by(&hub, Lsn(0));
    assert_eq!(
        (from_hub.ddl.len(), from_hub.rows.len()),
        (2, 2),
        "the hub forwards the whole unit"
    );
    let received = pulled_from(&edge, &hub, from_hub.clone());
    assert_ne!(hub.node_id(), author.node_id());

    let author_lineage = |key: &NaturalKey| {
        authored_lineages
            .iter()
            .find(|(table, natural_key, _, _)| table == "notes" && natural_key == key)
            .map(|(_, _, _, lineage)| lineage.clone())
            .expect("the author signed this row")
    };
    let authored_ddl = from_author
        .ddl
        .iter()
        .cloned()
        .map(DdlChange::from)
        .collect::<Vec<_>>();
    // The author's own identity for each schema record, exactly as the
    // author attested it when it first sent the record.
    let authored_identity = from_author
        .ddl_provenance
        .iter()
        .map(|provenance| {
            let named = provenance
                .author
                .as_ref()
                .expect("the author names itself on the schema it wrote");
            let signature = named.attestation.clone();
            assert!(!signature.is_empty(), "and signs it with its own key");
            crate::protocol::WireDdlAuthor {
                node_id: author.node_id(),
                incarnation: author.incarnation(),
                source_ddl_lsn: provenance.source_ddl_lsn,
                ordinal: provenance.ordinal,
                table: provenance.table.clone(),
                table_generation: provenance.table_generation,
                digest: provenance.digest.clone(),
                attestation: signature,
                unit_statements: named.unit_statements,
            }
        })
        .collect::<Vec<_>>();
    // The durable author identity a peer holds for each schema record, in
    // the order of its DDL log.
    let stored_authors = |peer: &JourneyPeer, when: &str| {
        let log = peer
            .db
            .changes_since(Lsn(0))
            .expect("read the declared changes");
        let mut next_ordinal = HashMap::<Lsn, u32>::new();
        log.ddl_lsn
            .iter()
            .map(|lsn| {
                let ordinal = next_ordinal.entry(*lsn).or_default();
                let key = Database::durable_received_ddl_arrival_key(*lsn, *ordinal);
                *ordinal += 1;
                peer.persisted::<DurableReceivedDdlArrival>(&key)
                    .unwrap_or_else(|| panic!("each received record is durable {when}"))
                    .author
                    .unwrap_or_else(|| panic!("each received record names its author {when}"))
            })
            .collect::<Vec<_>>()
    };

    let check = |edge: &JourneyPeer, when: &str| {
        // Rows: the same natural keys, and for each the author's signed
        // creator identity -- not the hub's.
        let mut stored_ids = edge
            .db
            .execute("SELECT id FROM notes", &HashMap::new())
            .unwrap_or_else(|error| panic!("the edge's table answers {when}: {error:?}"))
            .rows()
            .into_iter()
            .map(|row| row[0].clone())
            .collect::<Vec<_>>();
        stored_ids.sort_by_key(|value| format!("{value:?}"));
        let mut expected_ids = ids.iter().map(|id| Value::Uuid(*id)).collect::<Vec<_>>();
        expected_ids.sort_by_key(|value| format!("{value:?}"));
        assert_eq!(
            stored_ids, expected_ids,
            "the edge holds the author's rows {when}"
        );
        for id in ids {
            let key = NaturalKey::single("id".to_string(), Value::Uuid(id));
            let signed = author_lineage(&key);
            let stored = edge
                .persisted::<DurableRowLineageSidecar>(&Database::durable_row_lineage_config_key(
                    "notes",
                    &key,
                    signed.table_generation,
                ))
                .unwrap_or_else(|| panic!("the edge stored the row's lineage {when}"));
            assert_eq!(
                (
                    stored.author_node_id.as_str(),
                    stored.author_database_incarnation,
                    stored.author_local_mutation_position,
                    stored.table_generation,
                    stored.lineage_root.as_str(),
                    stored.lineage_attestation.as_slice(),
                ),
                (
                    signed.author_node_id.as_str(),
                    signed.author_database_incarnation,
                    signed.author_local_mutation_position,
                    signed.table_generation,
                    signed.lineage_root.as_str(),
                    signed.attestation.as_slice(),
                ),
                "the edge holds the AUTHOR's signed creator identity for {id} {when}"
            );
            assert_ne!(
                stored.author_node_id,
                hub.node_id(),
                "the forwarding hub is never recorded as the creator {when}"
            );
            Database::verify_lineage_attestation(
                &tenant,
                "notes",
                &key,
                &crate::protocol::WireRowLineage {
                    author_node_id: stored.author_node_id.clone(),
                    author_database_incarnation: stored.author_database_incarnation,
                    author_local_mutation_position: stored.author_local_mutation_position,
                    table_generation: stored.table_generation,
                    lineage_root: stored.lineage_root.clone(),
                    attestation: stored.lineage_attestation.clone(),
                },
            )
            .unwrap_or_else(|error| {
                panic!("the stored lineage verifies under the author's key {when}: {error:?}")
            });
            assert_eq!(
                edge.db
                    .accepted_sync_row_author("notes", &key)
                    .expect("read the accepted author"),
                Some(author.node_id()),
                "the accepted author of {id} is the author, not the hub {when}"
            );
        }
        // Schema: the author's statements in the author's order, with the
        // author's ordinals and table generations, each bound to a digest
        // that recomputes from the author's own statement.
        let log = edge
            .db
            .changes_since(Lsn(0))
            .expect("read the declared changes");
        assert_eq!(
            log.ddl.clone(),
            authored_ddl,
            "the edge's durable DDL log is the author's schema in the author's order {when}"
        );
        assert_durable_manifest_matches(edge, &received, when);
        assert_eq!(
            received.source_node_id,
            hub.node_id(),
            "the schema marker names the peer that delivered it"
        );
        let held = stored_authors(edge, when);
        assert!(
            held.iter()
                .all(|identity| identity.node_id != hub.node_id()),
            "the forwarding hub is never recorded as a schema record's author {when}"
        );
        assert_eq!(
            held, authored_identity,
            "every schema record the edge holds names the AUTHOR's node, incarnation, source \
                 LSN, ordinal, table generation and digest {when}"
        );
        for ((served, authored), ddl) in from_hub
            .ddl_provenance
            .iter()
            .zip(&from_author.ddl_provenance)
            .zip(&from_author.ddl)
        {
            assert_eq!(
                (served.ordinal, &served.table, served.table_generation),
                (authored.ordinal, &authored.table, authored.table_generation),
                "the forwarded record keeps the author's ordinal and table generation {when}"
            );
            assert_eq!(
                served.digest,
                crate::protocol::canonical_ddl_provenance_digest(
                    ddl,
                    served.source_ddl_lsn,
                    authored.ordinal,
                    authored.table.as_deref(),
                    authored.table_generation,
                )
                .expect("recompute the digest from the author's statement"),
                "the forwarded digest attests the author's own statement {when}"
            );
            let local_lsn = log.ddl_lsn[served.ordinal as usize];
            let generation = edge
                .db
                .recorded_ddl_generation_sidecar(local_lsn, served.ordinal)
                .expect("read the generation sidecar")
                .unwrap_or_else(|| panic!("the edge keeps each record's generation {when}"));
            assert_eq!(
                (generation.table, generation.table_generation),
                (authored.table.clone(), authored.table_generation),
                "the edge's durable record keeps the author's table generation {when}"
            );
        }
    };
    check(&edge, "after the second hop");
    let edge = edge.reopen();
    check(&edge, "after the edge restarts");

    // And the edge forwards the author's identity onward, not its own.
    let from_edge = served_by(&edge, Lsn(0));
    for (_, key, _, lineage) in crate::protocol::wire_row_lineages(&from_edge) {
        assert_eq!(
            lineage,
            author_lineage(&key),
            "a further hop still carries the author's signed lineage"
        );
    }
    assert_eq!(
        from_edge
            .ddl_provenance
            .iter()
            .map(|provenance| provenance.author.clone())
            .collect::<Vec<_>>(),
        authored_identity
            .iter()
            .cloned()
            .map(Some)
            .collect::<Vec<_>>(),
        "the edge forwards each schema record under the author's identity"
    );
    let far = JourneyPeer::open(root.path(), "forward-far-edge");
    let far_received = pulled_from(&far, &edge, from_edge);
    assert_eq!(far_received.source_node_id, edge.node_id());
    assert_eq!(
        stored_authors(&far, "at the fourth peer"),
        authored_identity,
        "a fourth peer that takes delivery from the edge holds the author's identity, not \
             the edge's or the hub's"
    );
    let far = far.reopen();
    assert_eq!(
        stored_authors(&far, "after the fourth peer restarts"),
        authored_identity,
        "the fourth peer keeps the author's identity across its restart"
    );
    far.db.close().expect("close the fourth peer");
    edge.db.close().expect("close the edge");
    hub.db.close().expect("close the hub");
    author.db.close().expect("close the author");
}

// ---- A peer's persisted-state fallback never re-authors received schema --
//
// When a peer's own logs no longer cover what it is asked for, it serves
// from its persisted state. Schema it RECEIVED must still leave it as the
// author wrote it: the author's statements, in the author's order, each
// under the author's own signature. The peer may sign only what it wrote
// itself; if the author's evidence is gone it refuses, naming the table,
// rather than restating someone else's migration under its own name.

/// The migration `author` wrote, as it first put it on the wire: its
/// statements in its order, and the signed identity block of each.
fn authored_migration(
    wire: &crate::protocol::WireChangeSet,
) -> (Vec<DdlChange>, Vec<crate::protocol::WireDdlAuthor>) {
    let ddl = wire.ddl.iter().cloned().map(DdlChange::from).collect();
    let authors = wire
        .ddl_provenance
        .iter()
        .map(|provenance| {
            provenance
                .author
                .clone()
                .expect("the author names itself on the schema it wrote")
        })
        .collect();
    (ddl, authors)
}

/// The author identity `peer` durably holds for each schema record, in
/// the order of its DDL log.
fn stored_ddl_authors(
    peer: &JourneyPeer,
    when: &str,
) -> Vec<Option<crate::protocol::WireDdlAuthor>> {
    let log = peer
        .db
        .changes_since(Lsn(0))
        .expect("read the declared changes");
    let mut next_ordinal = HashMap::<Lsn, u32>::new();
    log.ddl_lsn
        .iter()
        .map(|lsn| {
            let ordinal = next_ordinal.entry(*lsn).or_default();
            let key = Database::durable_received_ddl_arrival_key(*lsn, *ordinal);
            *ordinal += 1;
            peer.persisted::<DurableReceivedDdlArrival>(&key)
                .unwrap_or_else(|| panic!("each received record is durable {when}"))
                .author
        })
        .collect()
}

/// `holder` holds exactly the author's migration: the author's statements
/// in the author's order, each record naming the author's node,
/// incarnation, source LSN, ordinal, signed unit size, table generation
/// and digest, with a signature that verifies under the author's key.
fn assert_holds_the_authors_migration(
    holder: &JourneyPeer,
    author: &JourneyPeer,
    authored_ddl: &[DdlChange],
    authored_identity: &[crate::protocol::WireDdlAuthor],
    when: &str,
) {
    let tenant = TenantId::from(JOURNEY_TENANT);
    let log = holder
        .db
        .changes_since(Lsn(0))
        .expect("read the declared changes");
    assert_eq!(
        log.ddl, authored_ddl,
        "the holder's schema is the author's statements in the author's order {when}"
    );
    let held = stored_ddl_authors(holder, when);
    assert_eq!(
        held,
        authored_identity
            .iter()
            .cloned()
            .map(Some)
            .collect::<Vec<_>>(),
        "every schema record names the AUTHOR's node, incarnation, source LSN, ordinal, \
             unit size, table generation and digest -- not a forwarding peer's {when}"
    );
    for (ddl, named) in authored_ddl.iter().zip(authored_identity) {
        assert_eq!(named.node_id, author.node_id());
        crate::protocol::verify_ddl_author_attestation(
            &tenant,
            &crate::protocol::WireDdlChange::from(ddl.clone()),
            named,
        )
        .unwrap_or_else(|error| {
            panic!("each record's signature verifies under the author's key {when}: {error:?}")
        });
    }
}

/// A later structural migration must retain the first CREATE's exact
/// author evidence as well as its own across each relay and restart.
#[test]
fn separate_create_and_structural_alter_keep_original_signatures_through_further_hops() {
    let root = tempfile::TempDir::new().expect("scratch peer directory");
    let author = JourneyPeer::open(root.path(), "history-author");
    let hub = JourneyPeer::open(root.path(), "history-hub");

    author
        .db
        .execute(
            "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT)",
            &HashMap::new(),
        )
        .expect("author creates the table");
    let create = served_by(&author, Lsn(0));
    assert_eq!(create.ddl.len(), 1);
    pushed_to(&hub, &author, create);
    let create_lsn = author.db.current_lsn();

    let tx = author
        .db
        .begin()
        .expect("author starts the later migration");
    for statement in [
        "ALTER TABLE notes ADD COLUMN tag TEXT",
        "CREATE INDEX notes_tag ON notes (tag)",
    ] {
        author
            .db
            .execute_in_tx(tx, statement, &HashMap::new())
            .unwrap_or_else(|error| panic!("author runs `{statement}`: {error:?}"));
    }
    author
        .db
        .commit(tx)
        .expect("author commits the later migration");
    let later = served_by(&author, create_lsn);
    assert_eq!(later.ddl.len(), 2);
    pushed_to(&hub, &author, later);

    let original = served_by(&author, Lsn(0));
    let (authored_ddl, authored_identity) = authored_migration(&original);
    assert!(matches!(
        authored_ddl.as_slice(),
        [
            DdlChange::CreateTable { .. },
            DdlChange::AlterTable { .. },
            DdlChange::CreateIndex { .. }
        ]
    ));
    assert_ne!(
        authored_identity[0].source_ddl_lsn, authored_identity[1].source_ddl_lsn,
        "CREATE and ALTER are distinct authored transactions"
    );
    let authored_bytes = authored_identity
        .iter()
        .map(|identity| serde_json::to_vec(identity).expect("encode author block"))
        .collect::<Vec<_>>();
    let schema = crate::read_image::project_schema(
        "notes",
        &author
            .db
            .table_meta("notes")
            .expect("author table metadata"),
    );
    assert!(schema.columns.iter().any(|column| column.name == "tag"));
    assert!(schema.indexes.iter().any(|index| index.name == "notes_tag"));

    let check = |peer: &JourneyPeer, when: &str| {
        assert_holds_the_authors_migration(peer, &author, &authored_ddl, &authored_identity, when);
        assert_eq!(
            stored_ddl_authors(peer, when)
                .into_iter()
                .map(|identity| {
                    serde_json::to_vec(&identity.expect("received author block"))
                        .expect("encode stored author block")
                })
                .collect::<Vec<_>>(),
            authored_bytes,
            "stored author blocks and signature bytes stay exact {when}"
        );
        let received_schema = crate::read_image::project_schema(
            "notes",
            &peer
                .db
                .table_meta("notes")
                .expect("received table metadata"),
        );
        assert_eq!(
            received_schema, schema,
            ".schema agrees with the author {when}"
        );
        assert_eq!(
            served_by(peer, Lsn(0))
                .ddl_provenance
                .into_iter()
                .map(|entry| entry.author.expect("forwarded author block"))
                .collect::<Vec<_>>(),
            authored_identity,
            "outbound author order and signatures stay original {when}"
        );
    };

    check(&hub, "at the hub");
    let edge = JourneyPeer::open(root.path(), "history-edge");
    pulled_from(&edge, &hub, served_by(&hub, Lsn(0)));
    check(&edge, "at the edge");
    let edge = edge.reopen();
    check(&edge, "after the edge restarts");

    let far = JourneyPeer::open(root.path(), "history-far");
    pulled_from(&far, &edge, served_by(&edge, Lsn(0)));
    check(&far, "at the further edge");
    let final_peer = JourneyPeer::open(root.path(), "history-final");
    pulled_from(&final_peer, &far, served_by(&far, Lsn(0)));
    check(&final_peer, "after a further hop");

    final_peer.db.close().expect("close final peer");
    far.db.close().expect("close further edge");
    edge.db.close().expect("close edge");
    hub.db.close().expect("close hub");
    author.db.close().expect("close author");
}

/// Put `peer` on its persisted-state fallback the way a peer whose logs
/// no longer cover the requested cursor is: its change and schema logs
/// are gone while its durable store is intact.
#[cfg(any(test, feature = "test-seams"))]
fn force_persisted_state_fallback(peer: &JourneyPeer) {
    // Load the peer's history first so nothing reloads the logs later.
    let _ = peer
        .db
        .changes_since(Lsn(0))
        .expect("read the declared changes");
    peer.db.__force_sync_log_gap_for_test();
    assert!(
        peer.db.ddl_log.read().is_empty() && peer.db.change_log.read().is_empty(),
        "the peer's logs no longer cover the cursor, so it serves from persisted state"
    );
}

/// The author writes one migration -- create a table, alter it, index it,
/// in one transaction -- and pushes it to `hub`.
fn author_pushes_migration(
    author: &JourneyPeer,
    hub: &JourneyPeer,
) -> crate::protocol::WireChangeSet {
    let tx = author.db.begin().expect("the author opens one transaction");
    for statement in [
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT)",
        "ALTER TABLE notes ADD COLUMN tag TEXT",
        "CREATE INDEX notes_tag ON notes (tag)",
    ] {
        author
            .db
            .execute_in_tx(tx, statement, &HashMap::new())
            .unwrap_or_else(|error| panic!("the author runs `{statement}`: {error:?}"));
    }
    author
        .db
        .commit(tx)
        .expect("the author commits its migration");
    let from_author = served_by(author, Lsn(0));
    assert_eq!(from_author.ddl.len(), 3);
    let pushed = pushed_to(hub, author, from_author.clone());
    assert!(pushed.result.conflicts.is_empty(), "{:?}", pushed.result);
    from_author
}

/// A wiped edge restores from a hub that serves from its persisted state
/// after a restart. The edge must get the AUTHOR's migration -- its
/// statements, order, positions and signatures -- not a snapshot the hub
/// restated and signed as its own.
#[cfg(any(test, feature = "test-seams"))]
#[test]
fn a_restored_edge_gets_the_authors_migration_from_a_hub_serving_its_persisted_state() {
    let root = tempfile::TempDir::new().expect("scratch peer directory");
    let author = JourneyPeer::open(root.path(), "fallback-author");
    let hub = JourneyPeer::open(root.path(), "fallback-hub");
    let from_author = author_pushes_migration(&author, &hub);
    let (authored_ddl, authored_identity) = authored_migration(&from_author);

    let schema_lsn = author.db.current_lsn();
    let tx = author.db.begin().expect("the author opens one transaction");
    author
        .db
        .execute_in_tx(
            tx,
            "INSERT INTO notes (id, body, tag) VALUES ($id, 'note', 't')",
            &HashMap::from([("id".to_string(), Value::Uuid(uuid::Uuid::new_v4()))]),
        )
        .expect("the author writes a row");
    author.db.commit(tx).expect("the author commits its row");
    let pushed = pushed_to(&hub, &author, served_by(&author, schema_lsn));
    assert_eq!(pushed.result.applied_rows, 1);

    let hub = hub.reopen();
    force_persisted_state_fallback(&hub);
    let from_hub = served_by(&hub, Lsn(0));
    let restored = JourneyPeer::open(root.path(), "fallback-restored-edge");
    pulled_from(&restored, &hub, from_hub);
    assert_holds_the_authors_migration(
        &restored,
        &author,
        &authored_ddl,
        &authored_identity,
        "on the edge restored from the hub's persisted state",
    );
    let restored = restored.reopen();
    assert_holds_the_authors_migration(
        &restored,
        &author,
        &authored_ddl,
        &authored_identity,
        "after the restored edge restarts",
    );
    restored.db.close().expect("close the restored edge");
    hub.db.close().expect("close the hub");
    author.db.close().expect("close the author");
}

/// An edge that learned the author's migration from its old hub moves to a
/// new, empty hub, restarts, and re-uploads from its persisted state. The
/// new hub, and a wiped edge that restores from it, must hold the AUTHOR's
/// migration, never the moving edge's restatement of it.
#[cfg(any(test, feature = "test-seams"))]
#[test]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn a_destination_reupload_from_persisted_state_forwards_the_authors_migration() {
    let root = tempfile::TempDir::new().expect("scratch peer directory");
    let tenant = TenantId::from(JOURNEY_TENANT);
    let author = JourneyPeer::open(root.path(), "reupload-author");
    let old_hub = JourneyPeer::open(root.path(), "reupload-old-hub");
    let from_author = author_pushes_migration(&author, &old_hub);
    let (authored_ddl, authored_identity) = authored_migration(&from_author);

    let edge = JourneyPeer::open(root.path(), "reupload-edge");
    pulled_from(&edge, &old_hub, served_by(&old_hub, Lsn(0)));
    let new_hub = JourneyPeer::open(root.path(), "reupload-new-hub");
    edge.db
        .change_retention_sync_peer(&tenant, &new_hub.node_id())
        .expect("the edge moves to the new hub");
    let edge = edge.reopen();
    force_persisted_state_fallback(&edge);

    // The edge's push, step for step as the sync client builds it for a
    // destination re-upload.
    let (through, _) = edge
        .db
        .destination_reupload_epoch(&tenant, &new_hub.node_id())
        .expect("read the re-upload epoch")
        .expect("the move left a re-upload owed to the new hub");
    let fixture_operation_reader_36 = edge
        .db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");
    let mut fixture_operation_copy_36 =
        contextdb_engine::vector_read_memory::admission(fixture_operation_reader_36.clone());
    let mut fixture_selection_credit_36 =
        contextdb_core::read_memory::ReadCredit::new(fixture_operation_reader_36.clone());

    let (changes, arrivals, source) = edge
        .db
        .checked_changes_since_with_arrivals(
            Lsn(0),
            &mut fixture_operation_copy_36,
            fixture_operation_reader_36.clone(),
            &mut fixture_selection_credit_36,
            None,
        )
        .expect("the edge extracts its state");
    let changes = changes.filter_by_direction_history(
        &edge.db.sync_direction_history(),
        &[SyncDirection::Push, SyncDirection::Both],
    );
    let fixture_operation_reader_37 = edge
        .db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    let changes = edge
        .db
        .filter_outbound_received_ddl(
            changes,
            &source,
            Some(through),
            fixture_operation_reader_37.clone(),
        )
        .expect("the re-upload keeps the schema the edge held at the move");
    let changes = edge
        .db
        .append_destination_reupload_deletes(changes, through)
        .expect("the re-upload carries held deletes");
    let fixture_operation_reader_38 = edge
        .db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    let provenance = edge
        .db
        .outbound_ddl_provenance(
            &changes,
            &source,
            &tenant,
            &edge.node_id(),
            edge.incarnation(),
            &|bytes| Ok(edge.identity.sign_lineage(bytes)),
            fixture_operation_reader_38.clone(),
        )
        .expect("the edge attaches each record's provenance");
    let wire = crate::protocol::wire_changeset_with_arrivals_lineages_and_ddl_provenance(
        changes,
        &arrivals,
        &HashMap::new(),
        provenance,
    );
    let pushed = pushed_to(&new_hub, &edge, wire);
    assert!(pushed.result.conflicts.is_empty(), "{:?}", pushed.result);
    assert_holds_the_authors_migration(
        &new_hub,
        &author,
        &authored_ddl,
        &authored_identity,
        "on the new hub after the re-upload",
    );

    let restored = JourneyPeer::open(root.path(), "reupload-restored-edge");
    pulled_from(&restored, &new_hub, served_by(&new_hub, Lsn(0)));
    assert_holds_the_authors_migration(
        &restored,
        &author,
        &authored_ddl,
        &authored_identity,
        "on a wiped edge restored from the new hub",
    );
    restored.db.close().expect("close the restored edge");
    new_hub.db.close().expect("close the new hub");
    edge.db.close().expect("close the edge");
    old_hub.db.close().expect("close the old hub");
    author.db.close().expect("close the author");
}

/// When the hub no longer holds the author's signed identity for schema it
/// received, its persisted-state fallback refuses to serve that schema,
/// naming the table, and the restoring edge installs nothing. It never
/// restates the received migration under its own name.
#[cfg(any(test, feature = "test-seams"))]
#[test]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn a_hub_without_the_authors_evidence_refuses_to_serve_received_schema_from_persisted_state() {
    let root = tempfile::TempDir::new().expect("scratch peer directory");
    let author = JourneyPeer::open(root.path(), "evidence-author");
    let hub = JourneyPeer::open(root.path(), "evidence-hub");
    author_pushes_migration(&author, &hub);

    // The hub loses the author's signed identity for what it received.
    let log = hub
        .db
        .changes_since(Lsn(0))
        .expect("read the declared changes");
    let mut next_ordinal = HashMap::<Lsn, u32>::new();
    let mut stripped = Vec::new();
    for lsn in &log.ddl_lsn {
        let ordinal = next_ordinal.entry(*lsn).or_default();
        let key = Database::durable_received_ddl_arrival_key(*lsn, *ordinal);
        *ordinal += 1;
        let mut arrival = hub
            .persisted::<DurableReceivedDdlArrival>(&key)
            .expect("the hub recorded the received record");
        assert!(arrival.author.is_some());
        arrival.author = None;
        stripped.push((
            key,
            RedbPersistence::encode_config_value(&arrival).expect("encode the arrival"),
        ));
    }
    hub.db
        .persistence
        .as_ref()
        .expect("a file-backed hub")
        .flush_encoded_config_values(
            stripped
                .iter()
                .map(|(key, value)| (key.as_str(), value.clone()))
                .collect(),
        )
        .expect("drop the author's identity from the hub's records");

    let hub = hub.reopen();
    force_persisted_state_fallback(&hub);
    let tenant = TenantId::from(JOURNEY_TENANT);
    let fixture_operation_reader_39 = hub
        .db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");
    let mut fixture_operation_copy_39 =
        contextdb_engine::vector_read_memory::admission(fixture_operation_reader_39.clone());
    let mut fixture_selection_credit_39 =
        contextdb_core::read_memory::ReadCredit::new(fixture_operation_reader_39.clone());

    let served = hub
        .db
        .checked_changes_since_with_arrivals(
            Lsn(0),
            &mut fixture_operation_copy_39,
            fixture_operation_reader_39.clone(),
            &mut fixture_selection_credit_39,
            None,
        )
        .and_then(|(changes, arrivals, source)| {
            let fixture_operation_reader_40 = hub
                .db
                .declared_reader_memory()
                .expect("admit the actual fixture reader");

            let provenance = hub.db.outbound_ddl_provenance(
                &changes,
                &source,
                &tenant,
                &hub.node_id(),
                hub.incarnation(),
                &|bytes| Ok(hub.identity.sign_lineage(bytes)),
                fixture_operation_reader_40.clone(),
            )?;
            Ok(
                crate::protocol::wire_changeset_with_arrivals_lineages_and_ddl_provenance(
                    changes,
                    &arrivals,
                    &HashMap::new(),
                    provenance,
                ),
            )
        });
    let restored = JourneyPeer::open(root.path(), "evidence-restored-edge");
    match served {
        Err(Error::ReceivedSchemaAuthorshipUnavailable { table, .. }) => {
            assert_eq!(table, "notes", "the refusal names the table");
        }
        Err(other) => panic!("expected the typed authorship refusal, got {other:?}"),
        Ok(wire) => {
            let authors = wire
                .ddl_provenance
                .iter()
                .map(|provenance| provenance.author.as_ref().map(|a| a.node_id.clone()))
                .collect::<Vec<_>>();
            let installed = try_pulled_from(&restored, &hub, wire);
            panic!(
                "the hub served received schema it can no longer attribute to its author, \
                     signed by {authors:?} (hub is {}); the restoring edge's install: {:?}",
                hub.node_id(),
                installed.map(|_| "installed")
            );
        }
    }
    assert!(
        restored
            .db
            .changes_since(Lsn(0))
            .expect("read the declared changes")
            .ddl
            .is_empty()
            && restored.db.relational_store.table_meta.read().is_empty(),
        "the restoring edge installs nothing"
    );
    restored.db.close().expect("close the restored edge");
    hub.db.close().expect("close the hub");
    author.db.close().expect("close the author");
}

/// A unit that restates a populated table beside a real change to another
/// table leaves the restated table's identity exactly where it was. The
/// restated CREATE installs nothing, so nothing about that table changed,
/// and a receiver that handed it a new generation would disown every
/// lineage record the table's rows already carry.
#[test]
fn restating_a_populated_table_beside_a_real_change_keeps_its_generation() {
    let directory = tempfile::TempDir::new().expect("scratch receiver directory");
    let path = directory.path().join("restated.db");
    let policies = ConflictPolicies::uniform(ConflictPolicy::LatestWins);
    {
        let db = Database::open(&path).expect("open the receiver");
        db.execute(
            "CREATE TABLE kept (id INTEGER PRIMARY KEY)",
            &HashMap::new(),
        )
        .expect("the receiver's own table");
        db.execute(
            "CREATE TABLE other (id INTEGER PRIMARY KEY)",
            &HashMap::new(),
        )
        .expect("a second table the unit really changes");
        db.execute("INSERT INTO kept (id) VALUES (1)", &HashMap::new())
            .expect("populate the table the unit will restate");
        let before = db
            .authoritative_table_generation_counter("kept")
            .expect("read the restated table's generation");
        assert_eq!(before, 1, "the local CREATE minted the first generation");

        db.apply_changes(
            ChangeSet {
                ddl: vec![
                    unattested_create("kept"),
                    DdlChange::AlterTable {
                        name: "other".to_string(),
                        columns: vec![("note".to_string(), "TEXT".to_string())],
                        constraints: Vec::new(),
                        foreign_keys: Vec::new(),
                        composite_foreign_keys: Vec::new(),
                        composite_unique: Vec::new(),
                    },
                ],
                ddl_lsn: vec![Lsn(10), Lsn(11)],
                ..ChangeSet::default()
            },
            &policies,
        )
        .expect("a unit restating one table and changing another installs");
        assert_eq!(
            db.authoritative_table_generation_counter("kept")
                .expect("read the restated table's generation"),
            before,
            "a restated table keeps the generation it already had"
        );
        db.apply_changes(
            ChangeSet {
                rows: vec![unattested_row("kept", 2, Lsn(12))],
                ..ChangeSet::default()
            },
            &policies,
        )
        .expect("a later delivery to the restated table still applies");
        db.close().expect("close the receiver");
    }
    let reopened = Database::open(&path).expect("reopen the receiver");
    assert_eq!(
        reopened
            .authoritative_table_generation_counter("kept")
            .expect("read the restated table's generation after restart"),
        1,
        "the restated table's generation survives a restart unchanged"
    );
    reopened
        .apply_changes(
            ChangeSet {
                rows: vec![unattested_row("kept", 3, Lsn(13))],
                ..ChangeSet::default()
            },
            &policies,
        )
        .expect("a delivery after restart still applies to the restated table");
    forwarded_copy_holds(&reopened, "kept", 3);
}

/// Dropping and recreating one table twice in one unit advances its
/// generation once per real CREATE, in author order. Every CREATE reading
/// the same pre-unit counter would hand both incarnations one identity,
/// and a later same-name table could then be mistaken for one of them.
#[test]
fn dropping_and_recreating_a_table_twice_in_one_unit_advances_its_generation_in_order() {
    let directory = tempfile::TempDir::new().expect("scratch receiver directory");
    let path = directory.path().join("recreated.db");
    let policies = ConflictPolicies::uniform(ConflictPolicy::LatestWins);
    {
        let db = Database::open(&path).expect("open the receiver");
        db.execute(
            "CREATE TABLE cycled (id INTEGER PRIMARY KEY)",
            &HashMap::new(),
        )
        .expect("the receiver's own table");
        assert_eq!(
            db.authoritative_table_generation_counter("cycled")
                .expect("read the generation"),
            1
        );
        db.apply_changes(
            ChangeSet {
                ddl: vec![
                    DdlChange::DropTable {
                        name: "cycled".to_string(),
                    },
                    unattested_create("cycled"),
                    DdlChange::DropTable {
                        name: "cycled".to_string(),
                    },
                    unattested_create("cycled"),
                ],
                ddl_lsn: vec![Lsn(10), Lsn(11), Lsn(12), Lsn(13)],
                ..ChangeSet::default()
            },
            &policies,
        )
        .expect("a unit that drops and recreates one table twice installs");
        assert_eq!(
            db.authoritative_table_generation_counter("cycled")
                .expect("read the generation"),
            3,
            "each of the two real CREATEs advances the generation once, in order"
        );
        db.apply_changes(
            ChangeSet {
                rows: vec![unattested_row("cycled", 1, Lsn(14))],
                ..ChangeSet::default()
            },
            &policies,
        )
        .expect("a later delivery to the recreated table applies");
        db.close().expect("close the receiver");
    }
    let reopened = Database::open(&path).expect("reopen the receiver");
    assert_eq!(
        reopened
            .authoritative_table_generation_counter("cycled")
            .expect("read the generation after restart"),
        3,
        "the advanced generation survives a restart"
    );
    reopened
        .apply_changes(
            ChangeSet {
                rows: vec![unattested_row("cycled", 2, Lsn(15))],
                ..ChangeSet::default()
            },
            &policies,
        )
        .expect("a delivery after restart applies to the recreated table");
    forwarded_copy_holds(&reopened, "cycled", 2);
}

#[test]
fn received_table_drop_retires_old_generation_history_across_reopen() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("received-schema-drop-history.redb");
    let db = Database::open(&path).unwrap();
    let params = HashMap::new();
    db.execute(
        "CREATE TABLE retired (id INTEGER PRIMARY KEY, embedding VECTOR(2))",
        &params,
    )
    .unwrap();
    db.execute("CREATE TABLE kept (id INTEGER PRIMARY KEY)", &params)
        .unwrap();
    db.execute(
        "INSERT INTO retired (id, embedding) VALUES (1, [1.0, 0.0])",
        &params,
    )
    .unwrap();
    db.execute("INSERT INTO kept (id) VALUES (2)", &params)
        .unwrap();

    assert!(
        db.change_log_since(Lsn(0))
            .expect("read durable history")
            .iter()
            .any(|entry| {
                matches!(entry, ChangeLogEntry::RowInsert { table, .. } if table == "retired")
            })
    );
    assert!(db.change_log_since(Lsn(0)).expect("read durable history").iter().any(|entry| {
        matches!(entry, ChangeLogEntry::VectorInsert { index, .. } if index.table == "retired")
    }));
    assert!(
        db.change_log_since(Lsn(0))
            .expect("read durable history")
            .iter()
            .any(|entry| {
                matches!(entry, ChangeLogEntry::RowInsert { table, .. } if table == "kept")
            })
    );

    let drop_change = DdlChange::DropTable {
        name: "retired".to_string(),
    };
    let received_schema_read_memory_18 = db
        .declared_reader_memory()
        .expect("admit the received-schema operation reader");
    let mut received_schema_copy_owner_18 =
        contextdb_engine::vector_read_memory::admission(received_schema_read_memory_18.clone());

    db.commit_received_schema_stage_for_test(
        &ChangeSet {
            ddl: vec![drop_change.clone()],
            ddl_lsn: vec![Lsn(41)],
            ..ChangeSet::default()
        },
        &received_single_ddl(&drop_change, 1),
        &[],
        &HashMap::new(),
        false,
        &mut received_schema_copy_owner_18,
        received_schema_read_memory_18,
    )
    .expect("received drop commits one replacement image");

    assert!(
        !db.change_log_since(Lsn(0))
            .expect("read durable history")
            .iter()
            .any(|entry| match entry {
                ChangeLogEntry::RowInsert { table, .. }
                | ChangeLogEntry::RowDelete { table, .. } => {
                    table == "retired"
                }
                ChangeLogEntry::VectorInsert { index, .. }
                | ChangeLogEntry::VectorDelete { index, .. } => index.table == "retired",
                ChangeLogEntry::EdgeInsert { .. } | ChangeLogEntry::EdgeDelete { .. } => false,
            })
    );
    assert!(
        db.change_log_since(Lsn(0))
            .expect("read durable history")
            .iter()
            .any(|entry| {
                matches!(entry, ChangeLogEntry::RowInsert { table, .. } if table == "kept")
            })
    );
    let durable_coordinates = |database: &Database, table: &str| {
        let mut coordinates = database
            .change_log_since(Lsn(0))
            .expect("read durable table history")
            .into_iter()
            .filter_map(|entry| match entry {
                ChangeLogEntry::RowInsert {
                    table: name,
                    row_id,
                    lsn,
                }
                | ChangeLogEntry::RowDelete {
                    table: name,
                    row_id,
                    lsn,
                    ..
                } if name == table => Some((lsn, row_id)),
                _ => None,
            })
            .collect::<Vec<_>>();
        coordinates.sort();
        coordinates
    };
    assert!(durable_coordinates(&db, "retired").is_empty());
    assert!(!durable_coordinates(&db, "kept").is_empty());

    db.close().unwrap();
    drop(db);

    let reopened = Database::open(&path).unwrap();
    assert!(reopened.table_meta("retired").is_none());
    assert!(
        !reopened
            .change_log_since(Lsn(0))
            .expect("read durable history after reopen")
            .iter()
            .any(|entry| match entry {
                ChangeLogEntry::RowInsert { table, .. }
                | ChangeLogEntry::RowDelete { table, .. } => {
                    table == "retired"
                }
                ChangeLogEntry::VectorInsert { index, .. }
                | ChangeLogEntry::VectorDelete { index, .. } => index.table == "retired",
                ChangeLogEntry::EdgeInsert { .. } | ChangeLogEntry::EdgeDelete { .. } => false,
            })
    );
    assert!(
        reopened
            .change_log_since(Lsn(0))
            .expect("read durable history after reopen")
            .iter()
            .any(|entry| {
                matches!(entry, ChangeLogEntry::RowInsert { table, .. } if table == "kept")
            })
    );
    assert!(durable_coordinates(&reopened, "retired").is_empty());
    assert!(!durable_coordinates(&reopened, "kept").is_empty());
}

fn custody_cache_holds(db: &Database, key: &str) -> bool {
    db.custody_cache
        .lock()
        .as_ref()
        .is_some_and(|store| store.records.contains_key(key))
}

/// THE OBSERVATION RIGHT AFTER THE DURABLE COMMIT SEES NO PUBLICATION
/// COMPONENT APPLIED.
///
/// The received unit also retires one custody record, so its prepared
/// custody delta is the FIRST component publication applies. A fault at
/// `AfterDurableCommitBeforePublish` must find that delta still
/// unpublished in memory (the record still cached) while the durable
/// image already carries the new state, which a reopen serves whole.
#[test]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn the_fault_after_the_durable_commit_fires_before_any_publication_component() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("received-schema-before-publication.redb");
    let db = Database::open(&path).unwrap();
    db.execute(
        "DECLARE TENANT TABLE POLICY custody_marker SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST IMMUTABLE",
        &HashMap::new(),
    )
    .expect("a standalone store declares one tenant table policy");
    let policy_key = db
        .custody_cache
        .lock()
        .as_ref()
        .and_then(|store| {
            store
                .records
                .keys()
                .find(|key| key.starts_with("tenant_table_policy.v1."))
                .cloned()
        })
        .expect("the declared policy is in the live custody view");

    let change = create_table();
    let received = received_single_ddl(&change, 1);
    let retired_key = policy_key.clone();
    let mut prepare =
        move |_: &Database, ws: &mut WriteSet| -> Result<Vec<crate::custody::records::Record>> {
            ws.config_deletes.push(retired_key.clone());
            Ok(Vec::new())
        };
    RECEIVED_UNIT_FAULT.with(|fault| {
        fault.set(Some((
            ReceivedUnitFaultPoint::AfterDurableCommitBeforePublish,
            ReceivedUnitCopyFault::Unwind,
        )))
    });
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let fixture_operation_reader_3 = db
            .declared_reader_memory()
            .expect("admit the actual fixture reader");
        let mut fixture_operation_copy_3 =
            contextdb_engine::vector_read_memory::admission(fixture_operation_reader_3.clone());

        db.commit_received_schema_stage(
            &ChangeSet {
                ddl: vec![change.clone()],
                ddl_lsn: vec![Lsn(41)],
                ..ChangeSet::default()
            },
            Some(&received),
            &[],
            &HashMap::new(),
            SyncAdoption::Continuing,
            SyncApplyRole::PullLeg,
            None,
            false,
            None,
            None,
            Some(&mut prepare),
            None,
            None,
            |_| Ok(None),
            || Ok(()),
            &mut fixture_operation_copy_3,
            fixture_operation_reader_3.clone(),
        )
    }));
    RECEIVED_UNIT_FAULT.with(|fault| fault.set(None));
    assert!(
        outcome.is_err(),
        "the injected unwind must fire after the durable commit"
    );
    assert!(
        custody_cache_holds(&db, &policy_key),
        "the custody delta -- publication's first component -- was already published when \
             the after-durable-commit fault fired"
    );
    assert!(
        db.relational_store.table_meta("events").is_none(),
        "the relational projection was already published when the after-durable-commit \
             fault fired"
    );
    // The stop-serving rule (`PublicationInterruptionWatch`): the handle
    // refuses service until a reopen recovers the committed state.
    assert!(matches!(
        db.execute("SELECT id FROM custody_marker", &HashMap::new()),
        Err(Error::PublicationInterrupted)
    ));
    drop(db);

    let reopened = Database::open(&path).unwrap();
    assert!(
        reopened.table_meta("events").is_some(),
        "the durable image carried the new table; a reopen serves it"
    );
    let fixture_operation_reader_5 = reopened
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    assert!(
        reopened
            .custody_records(fixture_operation_reader_5.clone())
            .unwrap()
            .iter()
            .all(|record| record.key().unwrap() != policy_key),
        "the durable image carried the retired custody record; a reopen serves it retired"
    );
}

#[test]
fn received_schema_redb_failure_drops_stage_without_memory_or_disk_publication() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("received-schema-failure.redb");
    let db = Database::open(&path).unwrap();
    let (changes, received, lineages) = received_schema_create_with_row();
    crate::persistence::arm_received_schema_pre_commit_fault_for_test();
    let received_schema_read_memory_19 = db
        .declared_reader_memory()
        .expect("admit the received-schema operation reader");
    let mut received_schema_copy_owner_19 =
        contextdb_engine::vector_read_memory::admission(received_schema_read_memory_19.clone());

    assert!(
        db.commit_received_schema_stage_for_test(
            &changes,
            &received,
            &lineages,
            &HashMap::new(),
            false,
            &mut received_schema_copy_owner_19,
            received_schema_read_memory_19
        )
        .is_err()
    );

    assert!(db.received_schema_stages.lock().is_empty());
    assert!(db.table_meta("events").is_none());
    assert!(db.change_log.read().is_empty());
    assert!(db.change_log_table_index.read().is_empty());
    assert!(db.change_log_lsn_refcounts.read().is_empty());
    assert!(db.received_ddl_arrivals.read().is_empty());
    db.close().unwrap();
    drop(db);

    let reopened = Database::open(&path).unwrap();
    assert!(reopened.table_meta("events").is_none());
    assert!(reopened.change_log.read().is_empty());
    assert!(reopened.received_ddl_arrivals.read().is_empty());
}

fn assert_local_schema_transaction_absent(db: &Database, table: &str, trigger: &str, phase: &str) {
    assert!(
        db.table_meta(table).is_none(),
        "{phase}: failed local DDL must not publish table metadata"
    );
    assert!(
        db.list_triggers()
            .iter()
            .all(|declaration| declaration.name != trigger),
        "{phase}: failed local DDL must not publish a trigger"
    );
    let changes = db.changes_since(Lsn(0)).expect("read the declared changes");
    assert!(
        changes.ddl.is_empty() && changes.ddl_lsn.is_empty(),
        "{phase}: failed local DDL must not publish an authored schema vector"
    );
    assert_eq!(
        db.current_lsn(),
        Lsn(0),
        "{phase}: failed local DDL must not consume a commit frontier"
    );

    let persistence = db
        .persistence
        .as_ref()
        .expect("local schema failure proof uses a file-backed database");
    assert!(
        !persistence
            .load_all_table_meta()
            .unwrap()
            .contains_key(table),
        "{phase}: failed local DDL must not persist table metadata"
    );
    assert!(
        persistence.load_ddl_log().unwrap().is_empty(),
        "{phase}: failed local DDL must not persist a DDL log"
    );
    assert!(
        persistence.load_commit_index().unwrap().is_empty(),
        "{phase}: failed local DDL must not persist a commit-index frontier"
    );
    assert!(
        persistence
            .load_config_value::<u64>(&Database::durable_lineage_table_generation_key(table))
            .unwrap()
            .is_none(),
        "{phase}: failed local DDL must not persist a table generation"
    );
    let durable_triggers = persistence
        .load_config_value::<Vec<TriggerDeclaration>>("__trigger_declarations")
        .unwrap()
        .unwrap_or_default();
    assert!(
        durable_triggers
            .iter()
            .all(|declaration| declaration.name != trigger),
        "{phase}: failed local DDL must not persist a trigger declaration"
    );
}

#[test]
fn local_schema_memory_refusal_leaves_no_live_or_durable_schema() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("local-schema-memory-refusal.redb");
    let db = Database::open(&path).unwrap();
    db.set_memory_limit(Some(1)).unwrap();
    db.execute("BEGIN", &HashMap::new()).unwrap();

    let error = db
        .execute(
            "CREATE TABLE local_memory_refusal (id UUID PRIMARY KEY)",
            &HashMap::new(),
        )
        .expect_err("the schema-only stage must honor MEMORY_LIMIT");
    assert!(
        matches!(
            error,
            Error::MemoryBudgetExceeded {
                ref subsystem,
                ref operation,
                ..
            } if subsystem == "ddl" && operation == "stage_transactional_schema"
        ),
        "schema staging must return the typed memory refusal: {error:?}"
    );
    assert!(db.pending_local_schema_stages.lock().is_empty());
    assert!(db.local_schema_stages.lock().is_empty());
    // Durable verification needs its ordinary funding after the staging refusal.
    db.set_memory_limit(None).unwrap();
    assert_local_schema_transaction_absent(
        &db,
        "local_memory_refusal",
        "local_memory_refusal_insert",
        "before rollback",
    );
    db.execute("ROLLBACK", &HashMap::new()).unwrap();
    db.close().unwrap();
    drop(db);

    let reopened = Database::open(&path).unwrap();
    assert_local_schema_transaction_absent(
        &reopened,
        "local_memory_refusal",
        "local_memory_refusal_insert",
        "after reopen",
    );
}

#[test]
fn local_schema_commit_peak_memory_refusal_drops_private_projection() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("local-schema-commit-memory-refusal.redb");
    let db = Database::open(&path).unwrap();
    let params = HashMap::new();
    db.execute("BEGIN", &params).unwrap();
    db.execute(
        "CREATE TABLE local_commit_memory_refusal (id UUID PRIMARY KEY, body TEXT)",
        &params,
    )
    .unwrap();
    db.execute(
        "CREATE TRIGGER local_commit_memory_refusal_insert ON local_commit_memory_refusal WHEN INSERT",
        &params,
    )
    .unwrap();

    let staged_bytes = db.accountant.usage().used;
    assert!(staged_bytes > 0, "the private schema stage must be charged");
    db.set_memory_limit(Some(staged_bytes.saturating_add(1)))
        .unwrap();
    let error = db
        .execute("COMMIT", &params)
        .expect_err("commit must reserve its affected-schema projection before cloning");
    assert!(
        matches!(
            error,
            Error::MemoryBudgetExceeded {
                ref subsystem,
                ref operation,
                ..
            } if subsystem == "ddl" && operation == "commit_transactional_schema"
        ),
        "schema commit must return the typed peak-memory refusal: {error:?}"
    );
    assert!(db.pending_local_schema_stages.lock().is_empty());
    assert!(db.local_schema_stages.lock().is_empty());
    assert_local_schema_transaction_absent(
        &db,
        "local_commit_memory_refusal",
        "local_commit_memory_refusal_insert",
        "after commit memory refusal",
    );
    db.set_memory_limit(None).unwrap();
    db.close().unwrap();
    drop(db);

    let reopened = Database::open(&path).unwrap();
    assert_local_schema_transaction_absent(
        &reopened,
        "local_commit_memory_refusal",
        "local_commit_memory_refusal_insert",
        "after reopen",
    );
}

#[test]
fn local_schema_redb_failure_rolls_back_table_trigger_and_frontier() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("local-schema-redb-refusal.redb");
    let db = Database::open(&path).unwrap();
    let params = HashMap::new();
    db.execute("BEGIN", &params).unwrap();
    db.execute(
        "CREATE TABLE local_redb_refusal (id UUID PRIMARY KEY, body TEXT)",
        &params,
    )
    .unwrap();
    db.execute(
        "CREATE TRIGGER local_redb_refusal_insert ON local_redb_refusal WHEN INSERT",
        &params,
    )
    .unwrap();
    crate::persistence::arm_received_schema_pre_commit_fault_for_test();
    db.execute("COMMIT", &params)
        .expect_err("the shared Redb pre-commit seam must abort local schema durability");

    assert!(db.pending_local_schema_stages.lock().is_empty());
    assert!(db.local_schema_stages.lock().is_empty());
    assert_local_schema_transaction_absent(
        &db,
        "local_redb_refusal",
        "local_redb_refusal_insert",
        "before reopen",
    );
    db.close().unwrap();
    drop(db);

    let reopened = Database::open(&path).unwrap();
    assert_local_schema_transaction_absent(
        &reopened,
        "local_redb_refusal",
        "local_redb_refusal_insert",
        "after reopen",
    );
}

/// Run `statement` with a panic armed right after one local commit stage
/// registration, catch the unwind, and prove nothing the commit put in a
/// database-owned stage map outlives it: all three maps are empty and the
/// accountant is back to exactly what it held before the transaction.
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn assert_unwind_after_local_stage_registration_leaves_nothing(
    db: &Database,
    point: LocalCommitStageFaultPoint,
    statement: &str,
    used_before_transaction: usize,
) {
    LOCAL_COMMIT_STAGE_FAULT.with(|fault| fault.set(Some(point)));
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        db.execute(statement, &HashMap::new())
    }));
    let fired = LOCAL_COMMIT_STAGE_FAULT.with(|fault| fault.get().is_none());
    LOCAL_COMMIT_STAGE_FAULT.with(|fault| fault.set(None));
    assert!(
        fired,
        "{point:?}: the commit never reached this registration"
    );
    assert!(
        outcome.is_err(),
        "{point:?}: the injected panic must unwind through the commit"
    );
    assert!(
        db.local_schema_stages.lock().is_empty(),
        "{point:?}: an unwound commit left its local schema stage registered"
    );
    assert!(
        db.local_erasure_stages.lock().is_empty(),
        "{point:?}: an unwound commit left its local erasure stage registered"
    );
    assert!(
        db.vector_partition_snapshot_stages.lock().is_empty(),
        "{point:?}: an unwound commit left its vector snapshot sample registered"
    );
    assert_eq!(
        db.accountant.usage().used,
        used_before_transaction,
        "{point:?}: an unwound commit kept part of the declared memory limit spent"
    );
}

/// The stop-serving behavior for every door that arms this state.
///
/// Every fallible artifact is prepared before durability, so
/// publication after the durable commit does no fallible work. An unwind
/// there is an engine bug, not a resource outcome, and there is no
/// correct partial publication to return to -- this handle's memory no
/// longer matches its own durable image. The handle stops serving and a
/// reopen recovers the complete committed state from the durable image.
fn assert_refuses_service_after_interrupted_publication(db: &Database, table: &str) {
    let refusals = [
        (
            "a read",
            db.execute(&format!("SELECT id FROM {table}"), &HashMap::new())
                .map(|_| ()),
        ),
        (
            "a write",
            db.execute(
                &format!("INSERT INTO {table} VALUES (9001, 'after')"),
                &HashMap::new(),
            )
            .map(|_| ()),
        ),
        ("a new transaction", db.begin().map(|_| ())),
    ];
    for (what, result) in refusals {
        assert!(
            matches!(result, Err(Error::PublicationInterrupted)),
            "{what} on a handle whose publication was interrupted after the durable commit \
                 must be refused until a reopen recovers the committed state, got {result:?}"
        );
    }
}

/// Arm `point`, run `commit`, and prove the unwind went through
/// publication AFTER the durable commit, not before it.
fn unwind_at_publication(point: LocalCommitStageFaultPoint, commit: impl FnOnce()) {
    LOCAL_COMMIT_STAGE_FAULT.with(|fault| fault.set(Some(point)));
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(commit));
    let fired = LOCAL_COMMIT_STAGE_FAULT.with(|fault| fault.get().is_none());
    LOCAL_COMMIT_STAGE_FAULT.with(|fault| fault.set(None));
    assert!(
        fired,
        "{point:?}: the commit never reached its own publication boundary"
    );
    assert!(
        outcome.is_err(),
        "{point:?}: the injected unwind must unwind through publication"
    );
}

/// A LOCAL COMMIT WHOSE PUBLICATION IS INTERRUPTED STOPS SERVING, AND A
/// REOPEN RECOVERS THE WHOLE COMMITTED SCHEMA, NOT A PREFIX.
///
/// The received door has had this proof since the stop-serving rule
/// landed. The LOCAL door arms the same watch and had none.
#[test]
fn a_local_commit_whose_publication_is_interrupted_stops_serving_until_reopened() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("local-publication-interrupted.redb");
    let params = HashMap::new();
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE unwind_notes (id INTEGER PRIMARY KEY, body TEXT) SYNC OFF",
        &params,
    )
    .unwrap();
    db.execute("INSERT INTO unwind_notes VALUES (1, 'one')", &params)
        .unwrap();

    // One transaction carrying a whole little schema: a table, its own
    // index, and a second table that references it.
    db.execute("BEGIN", &params).unwrap();
    for ddl in [
        "CREATE TABLE unwind_ledger (id UUID PRIMARY KEY, label TEXT NOT NULL)",
        "CREATE INDEX unwind_ledger_label ON unwind_ledger (label)",
        "CREATE TABLE unwind_links (id UUID PRIMARY KEY, ledger_id UUID NOT NULL REFERENCES unwind_ledger(id))",
    ] {
        db.execute(ddl, &params).unwrap();
    }
    unwind_at_publication(
        LocalCommitStageFaultPoint::AfterDurableCommitBeforeLocalPublication,
        || {
            let _ = db.execute("COMMIT", &params);
        },
    );
    assert_refuses_service_after_interrupted_publication(&db, "unwind_notes");
    drop(db);

    let reopened = Database::open(&path).unwrap();
    for table in ["unwind_ledger", "unwind_links"] {
        assert!(
            reopened.table_meta(table).is_some(),
            "a reopen recovers the whole committed schema, never a prefix: {table} is missing"
        );
    }
    assert!(
        reopened.column_has_covering_index("unwind_ledger", "label"),
        "a reopen recovers the committed index with the tables it was authored beside"
    );
    reopened
        .execute(
            "INSERT INTO unwind_ledger VALUES ($id, 'after')",
            &HashMap::from([(
                "id".to_string(),
                Value::Uuid(uuid::Uuid::from_u128(0x10CA1)),
            )]),
        )
        .expect("the reopened handle serves the recovered schema");
    assert_eq!(
        reopened
            .execute("SELECT id FROM unwind_notes", &params)
            .unwrap()
            .rows()
            .len(),
        1,
        "and what the handle already held is still whole"
    );
    reopened.close().unwrap();
}

/// THE SAME RULE ON THE RESERVED-LSN COMMIT THAT STAMPS EXPIRED ROWS.
///
/// It takes the same durable apply and publishes its own in-memory commit
/// bookkeeping afterwards, and it did not arm the watch: a panic past its
/// durable apply left the handle serving a memory image behind its own
/// file.
#[test]
fn an_expiry_commit_whose_publication_is_interrupted_stops_serving_until_reopened() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("expiry-publication-interrupted.redb");
    let params = HashMap::new();
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE unwind_notes (id INTEGER PRIMARY KEY, body TEXT) RETAIN 1 DAYS SYNC OFF",
        &params,
    )
    .unwrap();
    for row in 1..=3 {
        db.execute(
            &format!("INSERT INTO unwind_notes VALUES ({row}, 'row-{row}')"),
            &params,
        )
        .unwrap();
    }
    let far_future = Wallclock::from_raw_wire(Wallclock::now().0 + 30 * 24 * 60 * 60 * 1000);
    unwind_at_publication(
        LocalCommitStageFaultPoint::AfterReservedLsnDurableCommit,
        || {
            let _ = db.maintenance_context().stamp_expired_rows(far_future);
        },
    );
    assert_refuses_service_after_interrupted_publication(&db, "unwind_notes");
    drop(db);

    let reopened = Database::open(&path).unwrap();
    assert!(
        reopened.table_meta("unwind_notes").is_some(),
        "a reopen recovers the complete committed state"
    );
    reopened
        .execute("INSERT INTO unwind_notes VALUES (4, 'after')", &params)
        .expect("the reopened handle serves again");
    reopened.close().unwrap();
}

fn local_stage_unwind_fixture(tmp: &tempfile::TempDir, name: &str) -> Database {
    let db = Database::open(tmp.path().join(name)).unwrap();
    let params = HashMap::new();
    db.execute(
        "CREATE TABLE unwind_notes (id INTEGER PRIMARY KEY, body TEXT) SYNC OFF",
        &params,
    )
    .unwrap();
    db.execute("INSERT INTO unwind_notes VALUES (1, 'one')", &params)
        .unwrap();
    db.execute("INSERT INTO unwind_notes VALUES (2, 'two')", &params)
        .unwrap();
    db
}

/// A transactional schema change: its stage carries a prepared memory
/// swap and a temporary reservation.
#[test]
fn a_panic_after_the_local_schema_stage_registration_leaves_no_stage_or_charge_behind() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db = local_stage_unwind_fixture(&tmp, "local-schema-unwind.redb");
    let params = HashMap::new();
    let before = db.accountant.usage().used;
    db.execute("BEGIN", &params).unwrap();
    db.execute(
        "CREATE TABLE unwind_schema (id UUID PRIMARY KEY, body TEXT)",
        &params,
    )
    .unwrap();
    assert_unwind_after_local_stage_registration_leaves_nothing(
        &db,
        LocalCommitStageFaultPoint::AfterLocalSchemaStageRegistration,
        "COMMIT",
        before,
    );
}

/// An ordinary row commit registers its registered-snapshot sample.
#[test]
fn a_panic_after_the_vector_snapshot_stage_registration_leaves_no_stage_or_charge_behind() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db = local_stage_unwind_fixture(&tmp, "local-snapshot-unwind.redb");
    let before = db.accountant.usage().used;
    assert_unwind_after_local_stage_registration_leaves_nothing(
        &db,
        LocalCommitStageFaultPoint::AfterVectorSnapshotStageRegistration,
        "DELETE FROM unwind_notes WHERE id = 1",
        before,
    );
}

/// A local discard registers its prepared erasure image, which holds a
/// vector workspace and a prepared memory swap.
#[test]
fn a_panic_after_the_local_erasure_stage_registration_leaves_no_stage_or_charge_behind() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db = local_stage_unwind_fixture(&tmp, "local-erasure-unwind.redb");
    let before = db.accountant.usage().used;
    assert_unwind_after_local_stage_registration_leaves_nothing(
        &db,
        LocalCommitStageFaultPoint::AfterLocalErasureStageRegistration,
        "DISCARD FROM unwind_notes WHERE id = 2",
        before,
    );
}

#[test]
fn received_schema_prepare_failure_drops_registered_stage_without_publication() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("received-schema-prepare-failure.redb");
    let db = Database::open(&path).unwrap();
    let (changes, received, lineages) = received_schema_create_with_row();
    let received_schema_read_memory_20 = db
        .declared_reader_memory()
        .expect("admit the received-schema operation reader");
    let mut received_schema_copy_owner_20 =
        contextdb_engine::vector_read_memory::admission(received_schema_read_memory_20.clone());

    assert!(
        db.commit_received_schema_stage_for_test(
            &changes,
            &received,
            &lineages,
            &HashMap::new(),
            true,
            &mut received_schema_copy_owner_20,
            received_schema_read_memory_20
        )
        .is_err()
    );

    assert!(db.received_schema_stages.lock().is_empty());
    assert!(db.table_meta("events").is_none());
    assert!(db.change_log.read().is_empty());
    assert!(db.change_log_table_index.read().is_empty());
    assert!(db.change_log_lsn_refcounts.read().is_empty());
    db.close().unwrap();
    drop(db);
    assert!(
        Database::open(&path)
            .unwrap()
            .table_meta("events")
            .is_none()
    );
}

#[test]
fn received_schema_delete_log_uses_latest_key_after_primary_key_update() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("received-schema-delete-key.redb");
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE events (id INTEGER PRIMARY KEY) SYNC CONFLICT KEEP LATEST",
        &HashMap::new(),
    )
    .unwrap();
    db.execute("INSERT INTO events (id) VALUES (1)", &HashMap::new())
        .unwrap();
    db.execute("UPDATE events SET id = 2 WHERE id = 1", &HashMap::new())
        .unwrap();
    // The local setup uses the legacy SQL DDL helper, so seed the
    // detached planner's authoritative-generation mirror explicitly.
    db.in_memory_table_generations
        .lock()
        .insert("events".to_string(), 1);
    let row_id = durable_observation::rows(&db, "events")
        .iter()
        .rev()
        .find(|row| row.values.get("id") == Some(&Value::Int64(2)))
        .unwrap()
        .row_id;
    let target_key = NaturalKey::single("id".to_string(), Value::Int64(2));
    let old_key = NaturalKey::single("id".to_string(), Value::Int64(1));
    let mut ddl = alter_table();
    if let DdlChange::AlterTable { constraints, .. } = &mut ddl {
        constraints.push("SYNC CONFLICT KEEP LATEST".to_string());
    }
    let received = received_single_ddl(&ddl, 3);
    let changes = ChangeSet {
        ddl: vec![ddl],
        ddl_lsn: vec![Lsn(41)],
        rows: vec![RowChange {
            table: "events".to_string(),
            natural_key: target_key.clone(),
            values: HashMap::from([("id".to_string(), Value::Int64(2))]),
            deleted: true,
            lsn: Lsn(42),
            created_at: None,
        }],
        ..ChangeSet::default()
    };
    let received_schema_read_memory_21 = db
        .declared_reader_memory()
        .expect("admit the received-schema operation reader");
    let mut received_schema_copy_owner_21 =
        contextdb_engine::vector_read_memory::admission(received_schema_read_memory_21.clone());

    db.commit_received_schema_stage_for_test(
        &changes,
        &received,
        &[(
            "events".to_string(),
            target_key.clone(),
            Lsn(42),
            lineage("delete-author", 502, 3),
        )],
        &HashMap::new(),
        false,
        &mut received_schema_copy_owner_21,
        received_schema_read_memory_21,
    )
    .unwrap();

    let (stage_lsn, logged_key) = durable_observation::changes(&db)
        .iter()
        .rev()
        .find_map(|entry| match entry {
            ChangeLogEntry::RowDelete {
                table,
                row_id: logged_row_id,
                natural_key,
                lsn,
            } if table == "events" && *logged_row_id == row_id => Some((*lsn, natural_key.clone())),
            _ => None,
        })
        .expect("received delete change-log entry");
    assert_eq!(logged_key, target_key);
    assert_ne!(logged_key, old_key);
    assert!(
        db.changes_since(Lsn(0))
            .expect("read the declared changes")
            .rows
            .iter()
            .any(|row| row.deleted && row.natural_key == target_key)
    );
    assert!(durable_observation::table_index(&db, "events").contains(&(stage_lsn, row_id)));
    assert!(
        durable_observation::root(&db)
            .change_log_occurrences(stage_lsn)
            .unwrap()
            >= 1
    );
    db.close().unwrap();
    drop(db);

    let reopened = Database::open(&path).unwrap();
    let reopened_key = durable_observation::changes(&reopened)
        .iter()
        .find_map(|entry| match entry {
            ChangeLogEntry::RowDelete {
                table,
                row_id: logged_row_id,
                natural_key,
                lsn,
            } if table == "events" && *logged_row_id == row_id && *lsn == stage_lsn => {
                Some(natural_key.clone())
            }
            _ => None,
        })
        .expect("reopened received delete change-log entry");
    assert_eq!(reopened_key, target_key);
    assert!(
        reopened
            .changes_since(Lsn(0))
            .expect("read the declared changes")
            .rows
            .iter()
            .any(|row| row.deleted && row.natural_key == target_key)
    );
    assert!(durable_observation::table_index(&reopened, "events").contains(&(stage_lsn, row_id)));
    assert!(
        durable_observation::root(&reopened)
            .change_log_occurrences(stage_lsn)
            .unwrap()
            >= 1
    );
}

#[test]
fn staged_receipt_watermarks_keep_other_keys_and_relay_preserves_creator() {
    // This raw codec fixture declares its own writer operation; it has no
    // database request whose ambient owner it may borrow.
    let _fixture_decode_scope = crate::read_image_memory::ImageMemory::enter_writer_operation(
        crate::memory_accounting::AccountantHandle::standalone(None, true).unwrap(),
    )
    .unwrap();

    let db = Database::open_memory();
    let _memory =
        crate::read_image_memory::ImageMemory::enter_writer_operation(db.accountant.clone())
            .unwrap();
    let tenant_watermark_key = "tenant-a".to_string();
    let watermark_key = Database::applied_push_watermark_node_incarnation_key(
        &TenantId::from("tenant-a"),
        "relay-node",
        Incarnation(9),
    );
    db.in_memory_applied_push_watermarks.lock().extend([
        (watermark_key.clone(), Lsn(7)),
        ("unrelated".to_string(), Lsn(91)),
    ]);
    let ddl = create_table();
    let received = received_single_ddl(&ddl, 3);
    let changes = ChangeSet {
        ddl: vec![ddl],
        ddl_lsn: vec![Lsn(41)],
        rows: vec![RowChange {
            table: "events".to_string(),
            natural_key: NaturalKey::single("id".to_string(), Value::Int64(1)),
            values: HashMap::from([("id".to_string(), Value::Int64(1))]),
            deleted: false,
            lsn: Lsn(42),
            created_at: None,
        }],
        ..ChangeSet::default()
    };
    let lineages = vec![(
        "events".to_string(),
        NaturalKey::single("id".to_string(), Value::Int64(1)),
        Lsn(42),
        lineage("original-author", 501, 3),
    )];
    let arrivals = HashMap::new();
    let received_schema_read_memory_22 = db
        .declared_reader_memory()
        .expect("admit the received-schema operation reader");
    let mut received_schema_copy_owner_22 =
        contextdb_engine::vector_read_memory::admission(received_schema_read_memory_22.clone());

    let lower = db
        .prepare_received_schema_stage_with_adjudication(
            Lsn(90),
            &changes,
            &received,
            &lineages,
            ReceivedSchemaAdjudicationInputs {
                push_outcome_key: None,
                apply_role: SyncApplyRole::PullLeg,
                arrivals: &arrivals,
                adoption: SyncAdoption::Continuing,
                receipt: Some(receipt(6)),
                dependency_complete: false,
                terminal_refusal_context: None,
                hub_local_author: None,
                unattested_policies: None,
                receiver_tx: TxId(37),
            },
            &mut received_schema_copy_owner_22,
            received_schema_read_memory_22,
        )
        .unwrap();
    assert!(
        !lower
            .mirrors
            .receipt_and_applied_watermarks
            .contains_key("unrelated")
    );
    assert_eq!(
        lower.mirrors.receipt_and_applied_watermarks[&watermark_key],
        Lsn(7)
    );
    assert_eq!(
        lower.mirrors.receipt_and_applied_watermarks[&tenant_watermark_key],
        Lsn(6)
    );
    let received_schema_read_memory_23 = db
        .declared_reader_memory()
        .expect("admit the received-schema operation reader");
    let mut received_schema_copy_owner_23 =
        contextdb_engine::vector_read_memory::admission(received_schema_read_memory_23.clone());

    let higher = db
        .prepare_received_schema_stage_with_adjudication(
            Lsn(91),
            &changes,
            &received,
            &lineages,
            ReceivedSchemaAdjudicationInputs {
                push_outcome_key: None,
                apply_role: SyncApplyRole::PullLeg,
                arrivals: &arrivals,
                adoption: SyncAdoption::Continuing,
                receipt: Some(receipt(8)),
                dependency_complete: false,
                terminal_refusal_context: None,
                hub_local_author: None,
                unattested_policies: None,
                receiver_tx: TxId(38),
            },
            &mut received_schema_copy_owner_23,
            received_schema_read_memory_23,
        )
        .unwrap();
    assert!(
        !higher
            .mirrors
            .receipt_and_applied_watermarks
            .contains_key("unrelated")
    );
    assert_eq!(
        higher.mirrors.receipt_and_applied_watermarks[&watermark_key],
        Lsn(8)
    );
    assert_eq!(
        higher.mirrors.receipt_and_applied_watermarks[&tenant_watermark_key],
        Lsn(8)
    );
    let durable_tenant_key = TenantId::from("tenant-a").config_key("sync_applied_push_watermark");
    assert!(
        higher
            .persistence
            .config_max_u64_keys
            .contains(&watermark_key)
    );
    assert!(
        higher
            .persistence
            .config_max_u64_keys
            .contains(&durable_tenant_key)
    );
    let (_, watermark) = higher
        .persistence
        .applied_push_watermark_values
        .iter()
        .find(|(key, _)| key == &watermark_key)
        .expect("receipt write stays in the applied-push watermark bucket");
    assert_eq!(
        RedbPersistence::decode_config_value::<u64>(watermark).unwrap(),
        8
    );
    let (_, lineage_value) = higher.persistence.lineage_values.first().unwrap();
    assert_eq!(
        RedbPersistence::decode_config_value::<DurableRowLineageSidecar>(lineage_value)
            .unwrap()
            .author_node_id,
        "original-author"
    );
    let (_, author_value) = higher.persistence.accepted_author_values.first().unwrap();
    assert_eq!(
        RedbPersistence::decode_config_value::<String>(author_value).unwrap(),
        "original-author"
    );
    assert_eq!(
        higher.mirrors.accepted_authors[&(
            "events".to_string(),
            sync_identity_key(
                "events",
                &NaturalKey::single("id".to_string(), Value::Int64(1))
            )
        )],
        "original-author"
    );
    db.publish_received_schema_stage(lower);
    {
        let watermarks = db.in_memory_applied_push_watermarks.lock();
        assert_eq!(watermarks["unrelated"], Lsn(91));
        assert_eq!(watermarks[&watermark_key], Lsn(7));
        assert_eq!(watermarks[&tenant_watermark_key], Lsn(6));
    }
    db.publish_received_schema_stage(higher);
    let watermarks = db.in_memory_applied_push_watermarks.lock();
    assert_eq!(watermarks["unrelated"], Lsn(91));
    assert_eq!(watermarks[&watermark_key], Lsn(8));
    assert_eq!(watermarks[&tenant_watermark_key], Lsn(8));
}

#[test]
fn file_reopen_hydrates_the_detached_received_schema_generation_snapshot() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("received-schema-generation-reopen.redb");
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE events (id INTEGER PRIMARY KEY, body TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE retired (id INTEGER PRIMARY KEY)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute("DROP TABLE retired", &HashMap::new()).unwrap();
    db.close().unwrap();
    drop(db);

    let db = Database::open(&path).unwrap();
    let generations = db
        .authoritative_table_generations_for_detached_working()
        .expect("the detached worker receives the complete durable generation snapshot");
    assert_eq!(generations.get("events"), Some(&1));
    assert_eq!(
        generations.get("retired"),
        Some(&1),
        "a dropped name remains so a later same-name CREATE cannot reuse generation one"
    );

    let mut ddl = alter_table();
    if let DdlChange::AlterTable { constraints, .. } = &mut ddl {
        constraints.push("SYNC CONFLICT KEEP LATEST".to_string());
    }
    let received_schema_read_memory_24 = db
        .declared_reader_memory()
        .expect("admit the received-schema operation reader");
    let mut received_schema_copy_owner_24 =
        contextdb_engine::vector_read_memory::admission(received_schema_read_memory_24.clone());

    db.commit_received_schema_stage_for_test(
        &ChangeSet {
            ddl: vec![ddl.clone()],
            ddl_lsn: vec![Lsn(41)],
            ..ChangeSet::default()
        },
        &received_single_ddl(&ddl, 1),
        &[],
        &HashMap::new(),
        false,
        &mut received_schema_copy_owner_24,
        received_schema_read_memory_24,
    )
    .expect("an authenticated DDL-only alter applies after the file-backed receiver reopens");
    assert_eq!(
        db.table_meta("events")
            .and_then(|meta| meta.conflict_policy),
        Some(contextdb_core::ConflictPolicy::KEEP_LATEST)
    );
    db.close().unwrap();
    drop(db);

    let reopened = Database::open(&path).unwrap();
    assert_eq!(
        reopened
            .table_meta("events")
            .and_then(|meta| meta.conflict_policy),
        Some(contextdb_core::ConflictPolicy::KEEP_LATEST),
        "the accepted received alteration remains durable after its own reopen"
    );
}

#[test]
fn detached_generation_snapshot_rejects_malformed_durable_u64() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("received-schema-generation-malformed.redb");
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE events (id INTEGER PRIMARY KEY)",
        &HashMap::new(),
    )
    .unwrap();
    let key = Database::durable_lineage_table_generation_key("events");
    db.persistence
        .as_ref()
        .unwrap()
        .flush_encoded_config_values(vec![(key.as_str(), Vec::new())])
        .unwrap();

    let err = db
        .authoritative_table_generations_for_detached_working()
        .expect_err("malformed generation bytes must fail before detached schema planning");
    assert!(matches!(err, Error::StoreCorrupted { path, .. } if path == key.as_str()));
}

#[test]
fn detached_generation_snapshot_rejects_canonical_u64_with_trailing_junk() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("received-schema-generation-trailing.redb");
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE events (id INTEGER PRIMARY KEY)",
        &HashMap::new(),
    )
    .unwrap();
    let key = Database::durable_lineage_table_generation_key("events");
    let mut trailing = RedbPersistence::encode_config_value(&1u64).unwrap();
    trailing.push(0);
    db.persistence
        .as_ref()
        .unwrap()
        .flush_encoded_config_values(vec![(key.as_str(), trailing)])
        .unwrap();

    let err = db
        .authoritative_table_generations_for_detached_working()
        .expect_err("an otherwise-valid generation with trailing bytes is corrupt");
    assert!(matches!(err, Error::StoreCorrupted { path, .. } if path == key.as_str()));
}

#[test]
fn staged_accepted_delete_restamps_only_its_tombstone_and_durable_lineage() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE events (id INTEGER PRIMARY KEY) SYNC CONFLICT KEEP LATEST",
        &HashMap::new(),
    )
    .unwrap();
    db.execute("INSERT INTO events (id) VALUES (1)", &HashMap::new())
        .unwrap();
    db.execute("INSERT INTO events (id) VALUES (3)", &HashMap::new())
        .unwrap();
    db.execute("INSERT INTO events (id) VALUES (4)", &HashMap::new())
        .unwrap();
    let local_row = db.relational_store.tables.read()["events"][0].clone();
    let fallback_row = db.relational_store.tables.read()["events"]
        .iter()
        .find(|row| row.values.get("id") == Some(&Value::Int64(3)))
        .cloned()
        .unwrap();
    let unaffected_row = db.relational_store.tables.read()["events"]
        .iter()
        .find(|row| row.values.get("id") == Some(&Value::Int64(4)))
        .cloned()
        .unwrap();
    let deleted_key = NaturalKey::single("id".to_string(), Value::Int64(1));
    let fallback_key = NaturalKey::single("id".to_string(), Value::Int64(3));
    let unrelated_key = NaturalKey::single("id".to_string(), Value::Int64(2));
    db.sync_tombstone_arrivals.write().insert(
        Database::sync_tombstone_key("events", &unrelated_key),
        SyncTombstoneArrival {
            arrival: Lsn(1),
            delete_lsn: Lsn(1),
            row_id: Some(RowId(444)),
            kind: contextdb_relational::store::SyncSourceKind::Pulled,
            accepted_local_order_invalidated: false,
        },
    );
    let mut ddl = alter_table();
    if let DdlChange::AlterTable { constraints, .. } = &mut ddl {
        constraints.push("SYNC CONFLICT KEEP LATEST".to_string());
    }
    let received = received_single_ddl(&ddl, 3);
    let changes = ChangeSet {
        ddl: vec![ddl],
        ddl_lsn: vec![Lsn(41)],
        rows: vec![
            RowChange {
                table: "events".to_string(),
                natural_key: deleted_key.clone(),
                values: HashMap::from([("id".to_string(), Value::Int64(1))]),
                deleted: true,
                // The detached snapshot's visibility seed commits at
                // LSN 1, so its data mutation commits at LSN 2. This
                // explicit source arrival collides numerically with that
                // detached LSN and must still survive the restamp.
                lsn: Lsn(42),
                created_at: None,
            },
            RowChange {
                table: "events".to_string(),
                natural_key: fallback_key.clone(),
                values: HashMap::from([("id".to_string(), Value::Int64(3))]),
                deleted: true,
                lsn: Lsn(43),
                created_at: None,
            },
        ],
        ..ChangeSet::default()
    };
    let arrivals = HashMap::from([(Lsn(42), Some(Lsn(2))), (Lsn(43), None)]);
    let received_schema_read_memory_25 = db
        .declared_reader_memory()
        .expect("admit the received-schema operation reader");
    let mut received_schema_copy_owner_25 =
        contextdb_engine::vector_read_memory::admission(received_schema_read_memory_25.clone());

    let stage = db
        .prepare_received_schema_stage_with_adjudication(
            Lsn(99),
            &changes,
            &received,
            &[
                (
                    "events".to_string(),
                    deleted_key.clone(),
                    Lsn(42),
                    lineage("delete-author", 502, 3),
                ),
                (
                    "events".to_string(),
                    fallback_key.clone(),
                    Lsn(43),
                    lineage("delete-author", 503, 3),
                ),
            ],
            ReceivedSchemaAdjudicationInputs {
                push_outcome_key: None,
                apply_role: SyncApplyRole::PullLeg,
                arrivals: &arrivals,
                adoption: SyncAdoption::Continuing,
                receipt: Some(receipt(42)),
                dependency_complete: false,
                terminal_refusal_context: None,
                hub_local_author: None,
                unattested_policies: None,
                receiver_tx: TxId(37),
            },
            &mut received_schema_copy_owner_25,
            received_schema_read_memory_25,
        )
        .unwrap();
    let tombstone =
        &stage.mirrors.source_state[&Database::sync_tombstone_key("events", &deleted_key)];
    assert_eq!(tombstone.delete_lsn, Lsn(99));
    assert_eq!(tombstone.arrival, Lsn(2));
    assert_eq!(tombstone.row_id, Some(local_row.row_id));
    assert_eq!(
        tombstone.kind,
        contextdb_relational::store::SyncSourceKind::Pulled
    );
    let fallback_tombstone =
        &stage.mirrors.source_state[&Database::sync_tombstone_key("events", &fallback_key)];
    assert_eq!(fallback_tombstone.delete_lsn, Lsn(99));
    assert_eq!(fallback_tombstone.arrival, Lsn(99));
    assert_eq!(fallback_tombstone.row_id, Some(fallback_row.row_id));
    assert_eq!(
        stage.mirrors.source_state[&Database::sync_tombstone_key("events", &unrelated_key)]
            .delete_lsn,
        Lsn(1)
    );
    let records = stage
        .persistence
        .config_values
        .iter()
        .filter_map(|(_, bytes)| {
            RedbPersistence::decode_config_value::<DurableLineageRecord>(bytes).ok()
        })
        .collect::<Vec<_>>();
    for row_id in [local_row.row_id, fallback_row.row_id] {
        let record = records
            .iter()
            .find(|record| record.local_row_id == Some(row_id))
            .expect("accepted delete durable lineage record");
        assert_eq!(record.delete_lsn, 99);
        assert_eq!(record.delete_obligation, DurableDeleteObligation::Accepted);
    }
    db.publish_received_schema_stage(stage);
    let tables = db.relational_store.tables.read();
    let deleted = tables["events"]
        .iter()
        .find(|row| row.row_id == local_row.row_id)
        .unwrap();
    assert_eq!(deleted.deleted_tx, Some(TxId(37)));
    let fallback_deleted = tables["events"]
        .iter()
        .find(|row| row.row_id == fallback_row.row_id)
        .unwrap();
    assert_eq!(fallback_deleted.deleted_tx, Some(TxId(37)));
    let preserved = tables["events"]
        .iter()
        .find(|row| row.row_id == unaffected_row.row_id)
        .unwrap();
    assert_eq!(preserved.created_tx, unaffected_row.created_tx);
    assert_eq!(preserved.deleted_tx, unaffected_row.deleted_tx);
}

#[test]
fn staged_terminal_refusal_override_clears_only_matching_marker_and_scan() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE events (id INTEGER PRIMARY KEY)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute("INSERT INTO events (id) VALUES (1)", &HashMap::new())
        .unwrap();
    let row = db.relational_store.tables.read()["events"][0].clone();
    let key = NaturalKey::single("id".to_string(), Value::Int64(1));
    let context = TerminalRefusalPullContext {
        tenant_id: TenantId::from("tenant-a"),
        hub_node_id: "hub-a".to_string(),
        generation: 1,
    };
    let matching_marker_key = Database::terminal_refusal_marker_key(&context, "events", &key);
    db.terminal_refusal_markers.write().insert(
        matching_marker_key.clone(),
        TerminalRefusalMarkerRecord {
            table: "events".to_string(),
            natural_key: key.clone(),
            lsn: row.lsn.0,
            row_id: row.row_id.0,
            active: true,
            generation: 1,
        },
    );
    let unrelated_marker_key = (
        "tenant-b".to_string(),
        "hub-b".to_string(),
        "other".to_string(),
        vec![9],
    );
    let unrelated_scan_key = ("tenant-b".to_string(), "hub-b".to_string());
    db.terminal_refusal_markers.write().insert(
        unrelated_marker_key.clone(),
        TerminalRefusalMarkerRecord {
            table: "other".to_string(),
            natural_key: key.clone(),
            lsn: 1,
            row_id: 1,
            active: true,
            generation: 1,
        },
    );
    db.terminal_refusal_scans.write().insert(
        Database::terminal_refusal_context_key(&context),
        TerminalRefusalScanState {
            source: None,
            next_lsn: Lsn(2),
            active: true,
            generation: 1,
        },
    );
    db.terminal_refusal_scans.write().insert(
        unrelated_scan_key.clone(),
        TerminalRefusalScanState {
            source: None,
            next_lsn: Lsn(3),
            active: true,
            generation: 1,
        },
    );
    let ddl = alter_table();
    let received = received_single_ddl(&ddl, 3);
    let changes = ChangeSet {
        ddl: vec![ddl],
        ddl_lsn: vec![Lsn(41)],
        rows: vec![RowChange {
            table: "events".to_string(),
            natural_key: key.clone(),
            values: HashMap::from([("id".to_string(), Value::Int64(1))]),
            deleted: true,
            lsn: Lsn(42),
            created_at: None,
        }],
        ..ChangeSet::default()
    };
    let arrivals = HashMap::new();
    let received_schema_read_memory_26 = db
        .declared_reader_memory()
        .expect("admit the received-schema operation reader");
    let mut received_schema_copy_owner_26 =
        contextdb_engine::vector_read_memory::admission(received_schema_read_memory_26.clone());

    assert!(
        db.prepare_received_schema_stage_with_adjudication(
            Lsn(99),
            &changes,
            &received,
            &[(
                "events".to_string(),
                key.clone(),
                Lsn(42),
                lineage("terminal-author", 503, 3),
            )],
            ReceivedSchemaAdjudicationInputs {
                push_outcome_key: None,
                apply_role: SyncApplyRole::PullLeg,
                arrivals: &arrivals,
                adoption: SyncAdoption::Continuing,
                receipt: Some(receipt(42)),
                dependency_complete: false,
                terminal_refusal_context: None,
                hub_local_author: None,
                unattested_policies: None,
                receiver_tx: TxId(37),
            },
            &mut received_schema_copy_owner_26,
            received_schema_read_memory_26
        )
        .is_err()
    );
    assert!(
        db.terminal_refusal_markers
            .read()
            .contains_key(&matching_marker_key)
    );
    let received_schema_read_memory_27 = db
        .declared_reader_memory()
        .expect("admit the received-schema operation reader");
    let mut received_schema_copy_owner_27 =
        contextdb_engine::vector_read_memory::admission(received_schema_read_memory_27.clone());

    let stage = db
        .prepare_received_schema_stage_with_adjudication(
            Lsn(99),
            &changes,
            &received,
            &[(
                "events".to_string(),
                key.clone(),
                Lsn(42),
                lineage("terminal-author", 503, 3),
            )],
            ReceivedSchemaAdjudicationInputs {
                push_outcome_key: None,
                apply_role: SyncApplyRole::PullLeg,
                arrivals: &arrivals,
                adoption: SyncAdoption::Continuing,
                receipt: Some(receipt(42)),
                dependency_complete: false,
                terminal_refusal_context: Some(&context),
                hub_local_author: None,
                unattested_policies: None,
                receiver_tx: TxId(37),
            },
            &mut received_schema_copy_owner_27,
            received_schema_read_memory_27,
        )
        .unwrap();
    let inactive = stage
        .persistence
        .terminal_values
        .iter()
        .find_map(|(_, bytes)| {
            RedbPersistence::decode_config_value::<TerminalRefusalMarkerRecord>(bytes).ok()
        })
        .expect("terminal marker clear is durable");
    assert!(!inactive.active);
    assert!(
        !stage
            .mirrors
            .terminal_markers
            .contains_key(&matching_marker_key)
    );
    assert!(
        !stage
            .mirrors
            .terminal_markers
            .contains_key(&unrelated_marker_key)
    );
    assert!(
        !stage
            .mirrors
            .terminal_scans
            .contains_key(&unrelated_scan_key)
    );
    db.publish_received_schema_stage(stage);
    assert!(
        db.relational_store.tables.read()["events"][0]
            .deleted_tx
            .is_some()
    );
    assert!(
        db.terminal_refusal_markers
            .read()
            .contains_key(&unrelated_marker_key)
    );
    assert!(
        db.terminal_refusal_scans
            .read()
            .contains_key(&unrelated_scan_key)
    );
    assert!(
        !db.terminal_refusal_markers
            .read()
            .contains_key(&matching_marker_key)
    );
}

include!("authored_occurrence_delivery_tests.rs");
