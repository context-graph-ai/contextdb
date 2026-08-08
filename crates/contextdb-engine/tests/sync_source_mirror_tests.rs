//! The server-path copies exist only for the frozen public-surface AST audit.
//! Engine owns the compiled implementation; byte equality makes a mirror
//! incapable of quietly becoming a second implementation.

use std::path::Path;

fn assert_exact_mirror(engine: &Path, server: &Path) {
    let canonical = std::fs::read(engine).expect("read canonical sync source");
    let mirror = std::fs::read(server).expect("read server-path sync audit mirror");
    assert_eq!(
        canonical,
        mirror,
        "{} must remain an exact noncompiled mirror of {}",
        server.display(),
        engine.display(),
    );
}

#[test]
fn server_sync_sources_are_exact_engine_audit_mirrors() {
    let engine = Path::new(env!("CARGO_MANIFEST_DIR"));
    let root = engine.ancestors().nth(2).expect("contextdb workspace root");
    let server = root.join("crates/contextdb-server/src");
    if !server.is_dir() {
        // A published engine crate deliberately contains no sibling server
        // package. The workspace gate below remains mandatory whenever that
        // audit mirror is present.
        return;
    }
    assert_exact_mirror(
        &engine.join("src/transport/iroh.rs"),
        &server.join("transport/iroh.rs"),
    );
    assert_exact_mirror(
        &engine.join("src/transport/large_request_staging.rs"),
        &server.join("transport/large_request_staging.rs"),
    );
}

#[test]
fn combined_schema_commit_keeps_event_bus_and_trigger_sidecar_ordinals_aligned() {
    let event_bus = include_str!("../src/database/event_bus.rs");
    let trigger = include_str!("../src/database/trigger.rs");
    let database = include_str!("../src/database.rs");
    let persistence = include_str!("../src/persistence.rs");

    assert!(
        event_bus.contains("self.ddl_generation_sidecar_values(lsn, ddl, 0)?"),
        "staged event-bus DDL must carry its leading immutable sidecars"
    );
    assert!(
        trigger.contains("start_ordinal: u32")
            && trigger.contains("self.ddl_generation_sidecar_values(lsn, ddl, start_ordinal)?"),
        "staged trigger DDL must receive the preceding event-bus ordinal count"
    );
    assert!(
        database.contains("let trigger_ddl_start =\n                                self.stage_event_bus_ddl_for_commit")
            && database.contains(
                "self.stage_trigger_ddl_for_commit(lsn, trigger_ddl, trigger_ddl_start)?;"
            ),
        "the transaction coordinator must pass the staged event-bus count to triggers"
    );
    assert!(
        persistence.contains(".event_bus\n                    .into_iter()")
            && persistence.contains(".trigger\n                            .into_iter()"),
        "the durable DDL log must retain the event-bus-then-trigger order"
    );
    assert!(
        event_bus.contains("publish_in_memory_ddl_generation_sidecars(")
            && trigger.contains("staged.persistence.start_ordinal")
            && trigger.contains("publish_in_memory_ddl_generation_sidecars("),
        "in-memory staged DDL must publish the same event-bus-first sidecar sequence"
    );
}

#[test]
fn local_schema_authoring_persists_one_projected_ddl_batch_before_memory_publish() {
    let executor = include_str!("../src/executor.rs");
    let database = include_str!("../src/database.rs");

    assert!(
        executor.contains("RETRY_LOCAL_SCHEMA_PROJECTION")
            && executor.contains("match execute_plan_once(db, plan, params, tx)"),
        "a concurrent schema edit must transparently rerun validation, not expose an operator retry"
    );
    assert!(
        executor.contains("db.persist_local_table_projection_and_ddl(")
            && executor.contains("db.publish_local_table_projection_and_ddl("),
        "local ALTER/INDEX paths must persist their projection before memory publication"
    );
    assert!(
        database.contains("including any CASCADEd index drops")
            && database.contains("ddl_generation_sidecar_values(lsn, ddl, 0)?"),
        "the projected durable batch must assign one same-LSN sidecar sequence"
    );
}

#[test]
fn local_schema_authoring_never_publishes_a_metadata_only_index_or_uses_a_vanished_index_as_empty()
{
    let executor = include_str!("../src/executor.rs");
    let database = include_str!("../src/database.rs");
    let relational = include_str!("../../contextdb-relational/src/store.rs");

    assert!(
        relational.contains("pub fn projected_index_storage(")
            && relational.contains("pub fn publish_table_projection(")
            && relational.contains("let mut all_indexes = self.indexes.write();")
            && relational.contains("let mut tables = self.tables.write();")
            && relational.contains("let mut table_meta = self.table_meta.write();"),
        "a projected table must build its postings first, then publish rows, metadata, and indexes together"
    );
    assert!(
        database.contains("RelationalStore::projected_index_storage(&meta, &rows)")
            && database
                .contains("store.publish_table_projection(name, meta, rows, projected_indexes)"),
        "the durable local-DDL publisher must use the coherent relational projection path"
    );
    assert!(
        executor
            .matches("let rows = scan_rows_for_select(db, table, snapshot, tx)?;")
            .count()
            >= 2,
        "both index-presence race windows must fall back to the normal full scan, not return empty rows"
    );
}

#[test]
fn local_structural_ddl_revalidates_the_complete_schema_under_the_allocation_lock() {
    let executor = include_str!("../src/executor.rs");

    assert!(
        executor.contains(
            "fn schema_snapshot_matches(db: &Database, expected: &HashMap<TableName, TableMeta>)"
        ) && executor
            .contains("let validated_schema = db.relational_store().table_meta.read().clone();")
            && executor.contains("if !schema_snapshot_matches(db, &validated_schema)"),
        "foreign-key-sensitive local DDL must compare the complete metadata map before publishing"
    );
    assert!(
        executor.contains("let table_generation = db.next_table_generation_for_create(&p.name)?;")
            && executor.contains("A generation belongs to the CREATE that actually won"),
        "CREATE must allocate its generation only after its schema baseline still matches"
    );
}

#[test]
fn statement_schema_publication_gate_spans_planning_execution_and_nested_reads() {
    let database = include_str!("../src/database.rs");

    assert!(
        database.contains("schema_publication_gate: Arc<RwLock<()>>")
            && database.contains("static SCHEMA_PUBLICATION_STACK")
            && database.contains("Arc::as_ptr(&self.schema_publication_gate) as usize"),
        "scoped handles and nested trigger/subquery execution must share one re-entrant schema gate"
    );
    assert!(
        database.contains(
            "self.enter_schema_publication_gate(Self::statement_changes_table_schema(stmt))"
        ) && database
            .contains("let _schema_publication = self.enter_schema_publication_gate(false);")
            && database.contains("Self::changeset_changes_table_schema(&changes)"),
        "SQL statements, EXPLAIN, and received table DDL must enter the publication barrier"
    );
    assert!(
        database.contains("Statement::CreateTable(_)")
            && database.contains("Statement::AlterTable(_)")
            && database.contains("Statement::DropIndex(_)")
            && database.contains("DdlChange::CreateTable { .. }")
            && database.contains("DdlChange::DropIndex { .. }"),
        "all local and received table-schema mutations must take the writer side"
    );
}

#[test]
fn outbound_changeset_extraction_holds_the_schema_gate_before_commit_sampling() {
    let database = include_str!("../src/database.rs");
    let checked_start = database
        .find("pub(crate) fn checked_changes_since_with_arrivals")
        .expect("checked outbound extraction");
    let checked_tail = &database[checked_start..];
    let checked_end = checked_tail
        .find("\n    pub(crate) fn sync_arrivals_for_changes")
        .expect("end of checked outbound extraction");
    let checked = &checked_tail[..checked_end];

    assert!(
        database.contains("fn changes_since_under_schema_gate(&self, since_lsn: Lsn) -> ChangeSet")
            && database.contains("self.changes_since_base(since_lsn).0")
            && database.contains("Take schema before changes_since_base takes the commit mutex"),
        "outbound extraction must acquire schema before its commit-locked base snapshot"
    );
    assert!(
        database
            .matches("let _schema_publication = self.enter_schema_publication_gate(false);")
            .count()
            >= 4,
        "public changes, arrival-bearing changes, checked sync extraction, and EXPLAIN must retain a schema read guard"
    );
    let guard = checked
        .find("self.enter_schema_publication_gate(false)")
        .expect("checked extraction schema guard");
    let pending = checked
        .find("self.durable_pending_deletes_since(since_lsn)?")
        .expect("durable pending-delete obligations");
    let base = checked
        .find("self.changes_since_base(since_lsn)")
        .expect("commit-locked base extraction");
    let reconcile = checked
        .find("Self::reconcile_durable_pending_deletes(base, pending)")
        .expect("durable pending-delete reconciliation");
    let arrivals = checked
        .find("self.sync_arrivals_for_changes(&changes)")
        .expect("sync arrivals");
    assert!(
        guard < pending && pending < base && base < reconcile && reconcile < arrivals,
        "checked extraction must keep pending-delete obligations and arrivals in one ordered schema-gate scope"
    );
}

#[test]
fn direct_public_reads_inherit_the_schema_publication_gate_from_open_operation() {
    let database = include_str!("../src/database.rs");

    assert!(
        database.contains("_schema_publication: Option<SchemaPublicationGuard<'a>>")
            && database
                .contains("let schema_publication = self.enter_schema_publication_gate(false);")
            && database.contains("_schema_publication: Some(schema_publication)"),
        "all outer public operations, including direct point lookups and scans, must retain the schema read gate"
    );
    assert!(
        database.contains("Lock order is schema publication -> public operation -> commit /")
            && database.contains("vector gates. This is also the order used by outer SQL")
            && database
                .contains("Local/received DDL already owns the write side before entering here"),
        "the shared direct-read gate must preserve schema-before-operation/commit/vector ordering without upgrades"
    );
}

#[test]
fn outbound_wire_evidence_is_prepared_before_the_schema_lease_is_released() {
    let database = include_str!("../src/database.rs");
    let client = include_str!("../src/sync_client.rs");
    let server = include_str!("../src/sync_server.rs");

    assert!(
        database.contains("pub(crate) fn enter_outbound_sync_schema_read(&self)")
            && database.contains("_inner: self.enter_schema_publication_gate(false)"),
        "authenticated sync orchestration must reuse the existing schema-publication read side"
    );

    let client_enter = client
        .find("let schema_read = self.db.enter_outbound_sync_schema_read();")
        .expect("push enters outbound schema lease");
    let client_extract = client[client_enter..]
        .find("checked_changes_since_with_arrivals(since)?")
        .map(|offset| client_enter + offset)
        .expect("push extracts under lease");
    let client_provenance = client[client_extract..]
        .find("outbound_ddl_provenance(&batch, &ddl_provenance_source)?")
        .map(|offset| client_extract + offset)
        .expect("push prepares DDL provenance under lease");
    let client_drop = client[client_provenance..]
        .find("drop(schema_read);")
        .map(|offset| client_provenance + offset)
        .expect("push drops lease");
    let client_await = client[client_drop..]
        .find("self.request_push(encoded).await")
        .map(|offset| client_drop + offset)
        .expect("push awaits transport after lease");
    assert!(
        client_enter < client_extract
            && client_extract < client_provenance
            && client_provenance < client_drop
            && client_drop < client_await,
        "push evidence preparation must finish before transport I/O"
    );

    let server_enter = server
        .find("let schema_read = db.enter_outbound_sync_schema_read();")
        .expect("pull enters outbound schema lease");
    let server_extract = server[server_enter..]
        .find("db.checked_changes_since_with_arrivals(request.since_lsn)?")
        .map(|offset| server_enter + offset)
        .expect("pull extracts under lease");
    let server_provenance = server[server_extract..]
        .find("db.outbound_ddl_provenance(&unit, &ddl_provenance_source)?")
        .map(|offset| server_extract + offset)
        .expect("pull prepares every dependency-unit provenance under lease");
    let server_drop = server[server_provenance..]
        .find("drop(schema_read);")
        .map(|offset| server_provenance + offset)
        .expect("pull drops lease");
    let server_await = server[server_drop..]
        .find("(req.responder)(payload)")
        .map(|offset| server_drop + offset)
        .expect("pull invokes transport after lease");
    assert!(
        server_enter < server_extract
            && server_extract < server_provenance
            && server_provenance < server_drop
            && server_drop < server_await,
        "pull evidence preparation must finish before transport I/O"
    );
}

#[test]
fn plugin_added_table_ddl_upgrades_before_apply_without_rerunning_the_plugin() {
    let database = include_str!("../src/database.rs");
    let apply = database
        .split_once("fn apply_changes_impl(")
        .map(|(_, body)| body)
        .expect("received apply implementation");

    let enter = apply
        .find("let entered_with_table_schema = Self::changeset_changes_table_schema(&changes);")
        .expect("apply classifies the incoming schema shape");
    let shared = apply[enter..]
        .find("Some(self.enter_schema_publication_gate(false))")
        .map(|offset| enter + offset)
        .expect("data-only apply begins under the shared schema lease");
    let plugin = apply[shared..]
        .find("self.plugin.on_sync_pull(&mut changes)?;")
        .map(|offset| shared + offset)
        .expect("plugin transforms the batch once");
    let final_validation = apply[plugin..]
        .find("Self::validate_public_changeset_ddl_lsn(&changes)?;")
        .map(|offset| plugin + offset)
        .expect("transformed DDL is revalidated");
    let upgrade_condition = apply[final_validation..]
        .find("&& Self::changeset_changes_table_schema(&changes)")
        .map(|offset| final_validation + offset)
        .expect("plugin-added table DDL selects the upgrade path");
    let release_operation = apply[upgrade_condition..]
        .find("drop(operation.take());")
        .map(|offset| upgrade_condition + offset)
        .expect("upgrade releases the public operation before the schema lease");
    let release_shared = apply[release_operation..]
        .find("drop(schema_publication.take());")
        .map(|offset| release_operation + offset)
        .expect("upgrade releases the shared schema lease");
    let acquire_write = apply[release_shared..]
        .find("self.enter_apply_changes_schema_publication_gate(\"apply_changes\")?")
        .map(|offset| release_shared + offset)
        .expect("upgrade enters the callback-safe schema writer");
    let reopen_operation = apply[acquire_write..]
        .find("self.open_operation_after_public_tx_control_wait(\"apply_changes\")?")
        .map(|offset| acquire_write + offset)
        .expect("upgrade reopens the public operation under the writer");
    assert!(
        enter < shared
            && shared < plugin
            && plugin < final_validation
            && final_validation < upgrade_condition
            && upgrade_condition < release_operation
            && release_operation < release_shared
            && release_shared < acquire_write
            && acquire_write < reopen_operation,
        "plugin-added structural DDL must upgrade in canonical lock order before apply"
    );
}
