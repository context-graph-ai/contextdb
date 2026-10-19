use super::*;

fn empty_params() -> HashMap<String, Value> {
    HashMap::new()
}

/// Author the fresh-edge bootstrap schema as three separate local commits,
/// exactly as a locally-installed consumer would: a structured `CREATE
/// TABLE` carrying a single-column foreign key plus a composite unique
/// constraint, a `CREATE TRIGGER ... INCLUDING SYNC` on it, and an `ALTER
/// TABLE` adding a column. Each statement commits on its own so the
/// bootstrap schema occupies several distinct local (Lsn, ordinal)
/// occurrences in the DDL log, the same shape a real installer produces.
fn author_local_bootstrap_schema(db: &Database) {
    db.execute(
        "CREATE TABLE bootstrap_owners (id INTEGER PRIMARY KEY, tenant TEXT)",
        &empty_params(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE bootstrap_notes (id INTEGER PRIMARY KEY, tenant TEXT, owner_id INTEGER REFERENCES bootstrap_owners(id), slug TEXT, UNIQUE (tenant, slug))",
        &empty_params(),
    )
    .unwrap();
    db.execute(
        "CREATE TRIGGER bootstrap_notes_audit ON bootstrap_notes WHEN INSERT INCLUDING SYNC",
        &empty_params(),
    )
    .unwrap();
    db.execute(
        "ALTER TABLE bootstrap_notes ADD COLUMN content TEXT",
        &empty_params(),
    )
    .unwrap();
}

/// Round-trip one locally-authored `DdlChange` through the same durable
/// config codec the hub itself persists received/authoritative DDL through.
/// The codec is exact, so this is the authored declaration, foreign keys and
/// composite keys included: the shape a hub serves back to an edge pulling
/// its full authoritative schema history.
fn hub_served_shape(change: &DdlChange) -> DdlChange {
    let encoded = RedbPersistence::encode_config_value(change).unwrap();
    RedbPersistence::decode_config_value(&encoded).unwrap()
}

/// Build the full-authoritative-history `ReceivedDdlContext` a hub would
/// serve for the given hub-served DDL vector, authenticated from a
/// source node identity distinct from anything this edge authored locally.
fn full_history_from_hub(
    served_ddl: Vec<DdlChange>,
) -> (ChangeSet, crate::protocol::ReceivedDdlContext) {
    let source_lsn = Lsn(1);
    let entries = served_ddl
        .iter()
        .enumerate()
        .map(|(ordinal, change)| {
            let table = Database::ddl_affected_table(change).map(str::to_string);
            let table_generation = table.as_ref().map(|_| 1);
            crate::protocol::ReceivedDdlEntry {
                source_ddl_lsn: source_lsn,
                ordinal: ordinal as u32,
                table: table.clone(),
                table_generation,
                digest: crate::protocol::canonical_ddl_provenance_digest(
                    &crate::protocol::WireDdlChange::from(change.clone()),
                    source_lsn,
                    ordinal as u32,
                    table.as_deref(),
                    table_generation,
                )
                .unwrap(),
                author: None,
            }
        })
        .collect();
    (
        ChangeSet {
            ddl_lsn: vec![source_lsn; served_ddl.len()],
            ddl: served_ddl,
            ..ChangeSet::default()
        },
        crate::protocol::ReceivedDdlContext {
            tenant_id: TenantId::from("bootstrap-echo-tenant"),
            source_node_id: "bootstrap-echo-hub".to_string(),
            source_incarnation: Incarnation(41),
            entries,
        },
    )
}

fn hub_receipt(received: &crate::protocol::ReceivedDdlContext) -> SyncApplyReceipt {
    SyncApplyReceipt {
        tenant_id: received.tenant_id.clone(),
        node_id: received.source_node_id.clone(),
        incarnation: received.source_incarnation,
        source_lsn: Lsn(1),
        dependency_complete: true,
        hub_contact_ms: None,
    }
}

fn apply_full_history(
    db: &Database,
    changes: &ChangeSet,
    received: &crate::protocol::ReceivedDdlContext,
) -> Result<crate::protocol::WireApplyResult> {
    let received_schema_read_memory_1 = db
        .declared_reader_memory()
        .expect("admit the received-schema operation reader");
    let mut received_schema_copy_owner_1 =
        contextdb_engine::vector_read_memory::admission(received_schema_read_memory_1.clone());

    db.commit_received_schema_stage_with_receipt_for_test(
        changes,
        received,
        &[],
        &HashMap::new(),
        Some(hub_receipt(received)),
        false,
        &mut received_schema_copy_owner_1,
        received_schema_read_memory_1,
    )
}

/// The same outbound-DDL classification `SyncClient::pending_push_change_count`
/// relies on, driven directly against the store rather than through the
/// network-facing `SyncClient` (which additionally needs a live transport,
/// tenant, and push watermark this regression has no use for).
fn outbound_pending_ddl(db: &Database) -> Vec<DdlChange> {
    let fixture_operation_reader_1 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");
    let mut fixture_operation_copy_1 =
        contextdb_engine::vector_read_memory::admission(fixture_operation_reader_1.clone());
    let mut fixture_selection_credit_1 =
        contextdb_core::read_memory::ReadCredit::new(fixture_operation_reader_1.clone());

    let (changes, _arrivals, provenance) = db
        .checked_changes_since_with_arrivals(
            Lsn(0),
            &mut fixture_operation_copy_1,
            fixture_operation_reader_1.clone(),
            &mut fixture_selection_credit_1,
            None,
        )
        .unwrap();
    let history = db.sync_direction_history();
    let changes =
        changes.filter_by_direction_history(&history, &[SyncDirection::Push, SyncDirection::Both]);
    let fixture_operation_reader_2 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    db.filter_outbound_received_ddl(
        changes,
        &provenance,
        None,
        fixture_operation_reader_2.clone(),
    )
    .unwrap()
    .ddl
}

fn reopen(db: Database, path: &std::path::Path) -> Database {
    db.close().unwrap();
    drop(db);
    Database::open(path).unwrap()
}

#[test]
fn locally_bootstrapped_schema_reaches_zero_outbound_after_identical_full_history_pull() {
    let temp = tempfile::TempDir::new().unwrap();
    let path = temp.path().join("bootstrap-echo-identical.redb");
    let db = Database::open(&path).unwrap();

    author_local_bootstrap_schema(&db);
    let local_ddl = db
        .changes_since(Lsn(0))
        .expect("read the declared changes")
        .ddl;
    assert_eq!(
        local_ddl.len(),
        4,
        "the bootstrap group is exactly the four authored statements"
    );

    let served_ddl: Vec<DdlChange> = local_ddl.iter().map(hub_served_shape).collect();
    let (changes, received) = full_history_from_hub(served_ddl);
    apply_full_history(&db, &changes, &received).unwrap();

    assert_eq!(
        outbound_pending_ddl(&db),
        Vec::new(),
        "a full authoritative pull of semantically identical schema must leave zero outbound DDL"
    );

    let db = reopen(db, &path);
    assert_eq!(
        outbound_pending_ddl(&db),
        Vec::new(),
        "zero outbound DDL must survive close+reopen, not just the in-memory apply"
    );
}

#[test]
fn divergent_local_schema_stays_pending_after_identical_full_history_pull() {
    let temp = tempfile::TempDir::new().unwrap();
    let path = temp.path().join("bootstrap-echo-divergent.redb");
    let db = Database::open(&path).unwrap();

    author_local_bootstrap_schema(&db);
    let bootstrap_ddl = db
        .changes_since(Lsn(0))
        .expect("read the declared changes")
        .ddl;

    db.execute(
        "CREATE TABLE bootstrap_orphan (id INTEGER PRIMARY KEY)",
        &empty_params(),
    )
    .unwrap();

    let served_ddl: Vec<DdlChange> = bootstrap_ddl.iter().map(hub_served_shape).collect();
    let (changes, received) = full_history_from_hub(served_ddl);
    apply_full_history(&db, &changes, &received).unwrap();

    let assert_only_orphan_pending = |db: &Database, when: &str| {
        let outbound = outbound_pending_ddl(db);
        assert_eq!(
            outbound.len(),
            1,
            "only the genuinely divergent local table should stay pending {when}, got {outbound:?}"
        );
        assert!(
            matches!(&outbound[0], DdlChange::CreateTable { name, .. } if name == "bootstrap_orphan"),
            "the surviving pending DDL must be the divergent bootstrap_orphan table {when}, got {:?}",
            outbound[0]
        );
    };

    assert_only_orphan_pending(&db, "immediately after apply");
    let db = reopen(db, &path);
    assert_only_orphan_pending(&db, "after reopen");
}

#[test]
fn mixed_local_schema_commit_stays_fully_pending() {
    let temp = tempfile::TempDir::new().unwrap();
    let path = temp.path().join("bootstrap-echo-mixed-commit.redb");
    let db = Database::open(&path).unwrap();

    // One local commit carrying two CREATE TABLE statements: one whose exact
    // shape the hub will also report as full history, and one this edge
    // alone ever authored. `execute_in_tx` stages sequential local schema
    // DDL against a private clone without publishing a half-transaction
    // schema, so both statements land at the same local Lsn under distinct
    // ordinals once `commit` runs.
    let tx = db.begin().unwrap();
    db.execute_in_tx(
        tx,
        "CREATE TABLE bootstrap_mixed_shared (id INTEGER PRIMARY KEY)",
        &empty_params(),
    )
    .unwrap();
    db.execute_in_tx(
        tx,
        "CREATE TABLE bootstrap_mixed_local_only (id INTEGER PRIMARY KEY)",
        &empty_params(),
    )
    .unwrap();
    db.commit(tx).unwrap();

    let committed_ddl = db
        .changes_since(Lsn(0))
        .expect("read the declared changes")
        .ddl;
    assert_eq!(
        committed_ddl.len(),
        2,
        "execute_in_tx must have produced one local commit with two DDL occurrences"
    );

    // The hub's full history only ever reports the shared table -- the
    // local-only table never left this edge, so no source entry exists for
    // it.
    let shared_ddl = committed_ddl
        .iter()
        .find(|ddl| matches!(ddl, DdlChange::CreateTable { name, .. } if name == "bootstrap_mixed_shared"))
        .cloned()
        .expect("the shared table must be among the committed DDL");
    let served_ddl = vec![hub_served_shape(&shared_ddl)];
    let (changes, received) = full_history_from_hub(served_ddl);
    apply_full_history(&db, &changes, &received).unwrap();

    let assert_whole_commit_pending = |db: &Database, when: &str| {
        let outbound = outbound_pending_ddl(db);
        assert_eq!(
            outbound.len(),
            2,
            "a local commit containing any locally-unique DDL must stay pending in its entirety (all-or-nothing) {when}, got {outbound:?}"
        );
    };

    assert_whole_commit_pending(&db, "immediately after apply");
    let db = reopen(db, &path);
    assert_whole_commit_pending(&db, "after reopen");
}

#[test]
fn a_hub_serves_the_foreign_keys_and_composite_keys_its_author_wrote() {
    let temp = tempfile::TempDir::new().unwrap();
    let path = temp.path().join("bootstrap-echo-exact-shape.redb");
    let db = Database::open(&path).unwrap();
    author_local_bootstrap_schema(&db);
    let notes = |ddl: &[DdlChange]| {
        ddl.iter()
            .find_map(|change| match change {
                DdlChange::CreateTable {
                    name,
                    foreign_keys,
                    composite_unique,
                    ..
                } if name == "bootstrap_notes" => {
                    Some((foreign_keys.clone(), composite_unique.clone()))
                }
                _ => None,
            })
            .expect("the authored CREATE TABLE is in schema history")
    };
    let authored = notes(
        &db.changes_since(Lsn(0))
            .expect("read the declared changes")
            .ddl,
    );
    assert_eq!(authored.0.len(), 1, "the author wrote one foreign key");
    assert_eq!(authored.0[0].child_column, "owner_id");
    assert_eq!(
        authored.1,
        vec![vec!["tenant".to_string(), "slug".to_string()]]
    );
    let served: Vec<DdlChange> = db
        .changes_since(Lsn(0))
        .expect("read the declared changes")
        .ddl
        .iter()
        .map(hub_served_shape)
        .collect();
    assert_eq!(
        notes(&served),
        authored,
        "the served history is the authored one"
    );
    let db = reopen(db, &path);
    assert_eq!(
        notes(
            &db.changes_since(Lsn(0))
                .expect("read the declared changes")
                .ddl
        ),
        authored,
        "history read back from the store is the authored one"
    );
}

#[test]
fn schema_history_after_reopen_is_the_authored_record_not_current_metadata() {
    let temp = tempfile::TempDir::new().unwrap();
    let path = temp.path().join("bootstrap-echo-authored-history.redb");
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE history_owners (id INTEGER PRIMARY KEY)",
        &empty_params(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE history_notes (id INTEGER PRIMARY KEY)",
        &empty_params(),
    )
    .unwrap();
    db.execute(
        "ALTER TABLE history_notes ADD COLUMN owner_id INTEGER REFERENCES history_owners(id)",
        &empty_params(),
    )
    .unwrap();
    let created_keys = |db: &Database| {
        db.changes_since(Lsn(0))
            .expect("read the declared changes")
            .ddl
            .iter()
            .find_map(|change| match change {
                DdlChange::CreateTable {
                    name, foreign_keys, ..
                } if name == "history_notes" => Some(foreign_keys.clone()),
                _ => None,
            })
            .expect("the CREATE TABLE is in schema history")
    };
    assert!(created_keys(&db).is_empty(), "the CREATE declared no key");
    let db = reopen(db, &path);
    assert!(
        created_keys(&db).is_empty(),
        "a key added later by ALTER is never grafted onto the CREATE after reopen"
    );
}
