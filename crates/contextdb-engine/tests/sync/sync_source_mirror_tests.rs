//! The server-path copies exist only for the frozen public-surface AST audit.
//! Engine owns the compiled implementation; byte equality makes a mirror
//! incapable of quietly becoming a second implementation.

use std::path::Path;

// Compare identifiers and punctuation so formatting cannot change a guard,
// while an ordinal such as 10 cannot match the required token 0.
fn source_tokens(source: &str) -> Vec<&str> {
    let mut tokens = Vec::new();
    let mut word_start = None;
    for (offset, ch) in source.char_indices() {
        if ch.is_alphanumeric() || ch == '_' {
            if word_start.is_none() {
                word_start = Some(offset);
            }
            continue;
        }
        if let Some(start) = word_start.take() {
            tokens.push(&source[start..offset]);
        }
        if !ch.is_whitespace() {
            tokens.push(&source[offset..offset + ch.len_utf8()]);
        }
    }
    if let Some(start) = word_start {
        tokens.push(&source[start..]);
    }
    let mut normalized = Vec::with_capacity(tokens.len());
    for (index, &token) in tokens.iter().enumerate() {
        // A call's optional trailing comma can change when rustfmt wraps it.
        if token != "," || tokens.get(index + 1) != Some(&")") {
            normalized.push(token);
        }
    }
    normalized
}

fn source_token_matches(source: &str, expected: &[&str]) -> usize {
    let source = source_tokens(source);
    let mut wanted = Vec::new();
    for &part in expected {
        wanted.extend(source_tokens(part));
    }
    if wanted.is_empty() {
        return 0;
    }
    source
        .windows(wanted.len())
        .filter(|window| *window == wanted.as_slice())
        .count()
}

fn source_has_tokens(source: &str, expected: &[&str]) -> bool {
    source_token_matches(source, expected) != 0
}

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
    let event_bus = include_str!("../../src/database/event_bus.rs");
    let trigger = include_str!("../../src/database/trigger.rs");
    let database = include_str!("../../src/database/commit_and_row_writes.rs");
    let persistence = include_str!("../../src/persistence/redb_flush.rs");

    assert!(
        event_bus.contains("self.ddl_generation_sidecar_values(lsn, ddl, 0, read_memory.clone())?"),
        "staged event-bus DDL must carry its leading immutable sidecars"
    );
    assert!(
        trigger.contains("start_ordinal: u32")
            && trigger.contains(
                "self.ddl_generation_sidecar_values(lsn, ddl, start_ordinal, read_memory.clone())?"
            ),
        "staged trigger DDL must receive the preceding event-bus ordinal count"
    );
    assert!(
        database.contains("let trigger_ddl_start =\n                                self.stage_event_bus_ddl_for_commit")
            && database.contains(
                "self.stage_trigger_ddl_for_commit(lsn, trigger_ddl, trigger_ddl_start, read_memory.clone())?;"
            ),
        "the transaction coordinator must pass the staged event-bus count to triggers"
    );
    assert!(
        persistence.contains(".event_bus\n                    .into_iter()")
            && persistence.contains(".trigger\n                            .into_iter()"),
        "the durable DDL log must retain the event-bus-then-trigger order"
    );
    assert!(
        source_has_tokens(
            event_bus,
            &[
                "self.prepare_staged_memory_schema_history(",
                "lsn, 0, ddl, &persistence.generation_sidecars, )?",
            ],
        ) && source_has_tokens(
            trigger,
            &[
                "self.prepare_staged_memory_schema_history(",
                "lsn, start_ordinal, ddl, &persistence.generation_sidecars, )?",
            ],
        ) && source_has_tokens(
            event_bus,
            &["self.publish_memory_schema_history(staged.memory_history);"],
        ) && source_has_tokens(
            trigger,
            &["self.publish_memory_schema_history(staged.memory_history);"],
        ) && source_has_tokens(
            database,
            &[
                "if pending_sink_queue_mutation.borrow().is_some() {",
                "self.publish_staged_event_bus_ddl_commit_under_queue_mutation_lease(lsn,);",
                "} else { self.publish_staged_event_bus_ddl_commit(lsn); }",
                "self.publish_staged_trigger_ddl_commit(lsn);",
            ],
        ),
        "in-memory staged DDL must publish the same event-bus-first sidecar sequence"
    );
}

#[test]
fn local_schema_authoring_persists_one_projected_ddl_batch_before_memory_publish() {
    let executor = include_str!("../../src/executor.rs");
    let database = include_str!("../../src/database.rs");

    assert!(
        executor.contains("match execute_plan_once(")
            && executor.contains(
                "Err(Error::Other(message)) if message == RETRY_LOCAL_SCHEMA_PROJECTION => continue"
            ),
        "a concurrent schema edit must transparently rerun validation, not expose an operator retry"
    );
    assert!(
        executor.contains("db.persist_local_table_projection_and_ddl(")
            && executor.contains("db.publish_local_table_projection_and_ddl("),
        "local ALTER/INDEX paths must persist their projection before memory publication"
    );
    assert!(
        database.contains("including any CASCADEd index drops")
            && database
                .contains("ddl_generation_sidecar_values(lsn, ddl, 0, read_memory.clone())?"),
        "the projected durable batch must assign one same-LSN sidecar sequence"
    );
}

#[test]
fn local_schema_authoring_never_publishes_a_metadata_only_index_or_uses_a_vanished_index_as_empty()
{
    let executor = include_str!("../../src/executor.rs");
    let database = include_str!("../../src/database/maintenance_and_limits.rs");
    let relational = include_str!("../../../contextdb-relational/src/store.rs");

    assert!(
        relational.contains("pub fn projected_index_storage(")
            && relational.contains("pub fn publish_table_projection(")
            && relational.contains("let mut all_indexes = self.indexes.write();")
            && relational.contains("let mut tables = self.tables.write();")
            && relational.contains("let mut table_meta = self.table_meta.write();"),
        "a projected table must build its postings first, then publish rows, metadata, and indexes together"
    );
    assert!(
        source_has_tokens(
            database,
            &[
                "RelationalStore::projected_index_storage_with_budget(",
                "meta, rows, self.finalization.accountant.read_budget()?, )",
            ],
        ) && source_has_tokens(
            database,
            &[
                "if !store.publish_table_projection_with_generation(",
                "name, meta, rows, projected_indexes, memory_swap.history.take(), )",
            ],
        ),
        "the durable local-DDL publisher must use the coherent relational projection path"
    );
    assert!(
        source_token_matches(
            executor,
            &[
                "let rows = scan_rows_for_select_statement(",
                "db, table, snapshot, tx,",
                "(access_mode == IndexScanAccessMode::Select).then_some(residual_filter).flatten(),",
                "params, vector_copy_owner, read_memory.clone(), )?;",
            ],
        ) >= 2,
        "both index-presence race windows must fall back to the normal full scan, not return empty rows"
    );
}

#[test]
fn local_structural_ddl_revalidates_the_complete_schema_under_the_allocation_lock() {
    let executor = include_str!("../../src/executor.rs");

    assert!(
        executor.contains(
            "fn schema_snapshot_matches(db: &Database, expected: &HashMap<TableName, TableMeta>)"
        ) && executor
            .contains("let validated_schema = db.relational_store().table_meta.read().clone();")
            && executor.contains("if !schema_snapshot_matches(db, &validated_schema)"),
        "foreign-key-sensitive local DDL must compare the complete metadata map before publishing"
    );
    assert!(
        executor.contains("db.next_table_generation_for_create(&p.name, read_memory.clone())?;")
            && executor.contains("A generation belongs to the CREATE that actually won"),
        "CREATE must allocate its generation only after its schema baseline still matches"
    );
}

#[test]
fn statement_schema_publication_gate_spans_planning_execution_and_nested_reads() {
    let database = include_str!("../../src/database.rs");

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
            && database.contains("Self::changeset_changes_table_schema(changes)"),
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

// Follow Database calls by their parsed signatures, rather than choosing a
// projected/selected helper spelling. This is a bounded structural guard for
// the extraction coordinator; the sync behavior tests cover the data itself.
fn extraction_methods(source: &str) -> std::collections::BTreeMap<String, syn::ImplItemFn> {
    let syntax = syn::parse_file(source).expect("parse extraction source");
    let mut methods = std::collections::BTreeMap::new();
    for item in syntax.items {
        if let syn::Item::Impl(item) = item {
            for member in item.items {
                if let syn::ImplItem::Fn(method) = member {
                    methods.insert(method.sig.ident.to_string(), method);
                }
            }
        }
    }
    methods
}

fn extraction_type_has(ty: &syn::Type, name: &str) -> bool {
    use syn::visit::Visit;
    struct Find<'a> {
        name: &'a str,
        found: bool,
    }
    impl<'ast> Visit<'ast> for Find<'_> {
        fn visit_path_segment(&mut self, segment: &'ast syn::PathSegment) {
            self.found |= segment.ident == self.name;
            syn::visit::visit_path_segment(self, segment);
        }
    }
    let mut find = Find { name, found: false };
    find.visit_type(ty);
    find.found
}

#[derive(Debug, PartialEq, Eq)]
enum ExtractionStep {
    Pending,
    Base,
    Reconcile,
    Arrivals,
}

struct ExtractionTrace<'a> {
    methods: &'a std::collections::BTreeMap<String, syn::ImplItemFn>,
    stack: Vec<String>,
    guards: Vec<String>,
    steps: Vec<ExtractionStep>,
}
impl ExtractionTrace<'_> {
    fn walk(&mut self, name: &str) {
        use syn::visit::Visit;
        assert!(
            self.stack.len() < 16,
            "extraction helper depth exceeds audited boundary"
        );
        assert!(
            !self.stack.iter().any(|parent| parent == name),
            "recursive extraction helper: {name}"
        );
        let method = self
            .methods
            .get(name)
            .unwrap_or_else(|| panic!("missing extraction method {name}"));
        self.stack.push(name.to_owned());
        self.visit_block(&method.block);
        self.stack.pop();
    }

    fn call(&mut self, name: &str) {
        let Some(method) = self.methods.get(name) else {
            return;
        };
        let syn::ReturnType::Type(_, output) = &method.sig.output else {
            return;
        };
        let pending_input = method.sig.inputs.iter().any(|input| {
            matches!(input, syn::FnArg::Typed(arg) if extraction_type_has(&arg.ty, "DurablePendingDelete"))
        });
        let step = if extraction_type_has(output, "DurablePendingDelete") {
            Some(ExtractionStep::Pending)
        } else if extraction_type_has(output, "ChangeSet")
            && extraction_type_has(output, "DdlProvenanceSource")
            && !extraction_type_has(output, "HashMap")
        {
            Some(ExtractionStep::Base)
        } else if pending_input && extraction_type_has(output, "ChangeSet") {
            Some(ExtractionStep::Reconcile)
        } else if name == "sync_arrivals_for_changes_within" {
            Some(ExtractionStep::Arrivals)
        } else {
            None
        };
        if let Some(step) = step {
            assert!(
                !self.guards.is_empty(),
                "{step:?} must execute under a retained schema-reading operation; helper path {:?}",
                self.stack
            );
            self.steps.push(step);
        } else if extraction_type_has(output, "ChangeSet") {
            // Coordinator wrappers may move or be renamed. Follow their body,
            // not a hardcoded projected/selected alternative list.
            self.walk(name);
        }
    }
}
impl<'ast> syn::visit::Visit<'ast> for ExtractionTrace<'_> {
    fn visit_block(&mut self, block: &'ast syn::Block) {
        let retained = self.guards.len();
        syn::visit::visit_block(self, block);
        self.guards.truncate(retained);
    }

    fn visit_local(&mut self, local: &'ast syn::Local) {
        syn::visit::visit_local(self, local);
        if let Some(init) = &local.init {
            let expr = match init.expr.as_ref() {
                syn::Expr::Try(expr) => expr.expr.as_ref(),
                expr => expr,
            };
            if let syn::Expr::MethodCall(call) = expr
                && call.method == "open_operation"
                && let syn::Pat::Ident(binding) = &local.pat
            {
                self.guards.push(binding.ident.to_string());
            }
        }
    }

    fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
        // Arguments execute before the called helper.
        syn::visit::visit_expr_method_call(self, call);
        if matches!(call.receiver.as_ref(), syn::Expr::Path(path) if path.path.is_ident("self")) {
            self.call(&call.method.to_string());
        }
    }

    fn visit_expr_call(&mut self, call: &'ast syn::ExprCall) {
        syn::visit::visit_expr_call(self, call);
        if let syn::Expr::Path(path) = call.func.as_ref() {
            if path.path.is_ident("drop") {
                for arg in &call.args {
                    if let syn::Expr::Path(arg) = arg {
                        self.guards.retain(|name| !arg.path.is_ident(name.as_str()));
                    }
                }
            } else if path
                .path
                .segments
                .first()
                .is_some_and(|part| part.ident == "Self")
            {
                self.call(&path.path.segments.last().unwrap().ident.to_string());
            }
        }
    }
}

#[test]
fn outbound_changeset_extraction_holds_the_schema_gate_before_commit_sampling() {
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let source = |name: &str| {
        std::fs::read_to_string(source_root.join(name)).expect("read extraction source")
    };
    let database = source("database.rs");
    let mut methods = extraction_methods(&database);
    methods.extend(extraction_methods(&source("database/row_lineage.rs")));
    methods.extend(extraction_methods(&source("database/sync_outbound.rs")));
    // Prove that the primitive used by the trace really acquires the read
    // side and stores that same lease in its returned operation guard.
    use syn::visit::Visit;
    #[derive(Default)]
    struct SchemaOperationProof {
        leases: Vec<String>,
        retained: bool,
    }
    impl<'ast> Visit<'ast> for SchemaOperationProof {
        fn visit_local(&mut self, local: &'ast syn::Local) {
            syn::visit::visit_local(self, local);
            if let (syn::Pat::Ident(binding), Some(init)) = (&local.pat, &local.init)
                && let syn::Expr::MethodCall(call) = init.expr.as_ref()
                && call.method == "enter_schema_publication_gate"
                && matches!(call.args.first(), Some(syn::Expr::Lit(lit)) if matches!(&lit.lit, syn::Lit::Bool(value) if !value.value))
            {
                self.leases.push(binding.ident.to_string());
            }
        }
        fn visit_expr_call(&mut self, call: &'ast syn::ExprCall) {
            syn::visit::visit_expr_call(self, call);
            if matches!(call.func.as_ref(), syn::Expr::Path(path) if path.path.is_ident("drop")) {
                for arg in &call.args {
                    if let syn::Expr::Path(path) = arg {
                        self.leases
                            .retain(|lease| !path.path.is_ident(lease.as_str()));
                    }
                }
            }
        }
        fn visit_expr_struct(&mut self, value: &'ast syn::ExprStruct) {
            if value.path.is_ident("DatabaseOperationGuard") {
                for field in &value.fields {
                    if matches!(&field.member, syn::Member::Named(name) if name == "_schema_publication")
                        && let syn::Expr::Call(call) = &field.expr
                        && matches!(call.func.as_ref(), syn::Expr::Path(path) if path.path.is_ident("Some"))
                    {
                        self.retained |= call.args.iter().any(|arg| matches!(arg, syn::Expr::Path(path) if self.leases.iter().any(|lease| path.path.is_ident(lease.as_str()))));
                    }
                }
            }
            syn::visit::visit_expr_struct(self, value);
        }
    }
    let mut proof = SchemaOperationProof::default();
    proof.visit_block(
        &methods
            .get("open_operation")
            .expect("public operation primitive")
            .block,
    );
    assert!(
        proof.retained,
        "open_operation must retain its acquired schema read lease"
    );
    for root in [
        "changes_since_with_read_memory",
        "changes_since_with_arrivals_with_read_memory",
        "checked_changes_since_with_arrivals",
    ] {
        let mut trace = ExtractionTrace {
            methods: &methods,
            stack: Vec::new(),
            guards: Vec::new(),
            steps: Vec::new(),
        };
        trace.walk(root);
        if root == "changes_since_with_read_memory" {
            assert_eq!(
                trace.steps,
                [
                    ExtractionStep::Base,
                    ExtractionStep::Pending,
                    ExtractionStep::Reconcile
                ],
                "{root}"
            );
        } else {
            assert_eq!(
                trace.steps,
                [
                    ExtractionStep::Pending,
                    ExtractionStep::Base,
                    ExtractionStep::Reconcile,
                    ExtractionStep::Arrivals
                ],
                "{root}"
            );
        }
    }
    // Inspect the actual pending-push call site to discover its extraction
    // entry point. A wrapper rename must not silently remove pending coverage.
    struct PendingEntry {
        calls: Vec<String>,
    }
    impl<'ast> Visit<'ast> for PendingEntry {
        fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
            if matches!(call.receiver.as_ref(), syn::Expr::Field(field) if matches!(&field.member, syn::Member::Named(name) if name == "db"))
            {
                self.calls.push(call.method.to_string());
            }
            syn::visit::visit_expr_method_call(self, call);
        }
    }
    let client = extraction_methods(&source("sync_client.rs"));
    let pending = client
        .get("pending_push_changes")
        .expect("pending push extraction");
    let mut entry = PendingEntry { calls: Vec::new() };
    entry.visit_block(&pending.block);
    let mut found = 0;
    for name in entry.calls {
        let Some(method) = methods.get(&name) else {
            continue;
        };
        let syn::ReturnType::Type(_, output) = &method.sig.output else {
            continue;
        };
        if extraction_type_has(output, "ChangeSet")
            && extraction_type_has(output, "DdlProvenanceSource")
        {
            let mut trace = ExtractionTrace {
                methods: &methods,
                stack: Vec::new(),
                guards: Vec::new(),
                steps: Vec::new(),
            };
            trace.walk(&name);
            assert_eq!(
                trace.steps,
                [
                    ExtractionStep::Pending,
                    ExtractionStep::Base,
                    ExtractionStep::Reconcile,
                    ExtractionStep::Arrivals
                ],
                "pending push via {name}"
            );
            found += 1;
        }
    }
    assert_eq!(
        found, 1,
        "pending push must call one audited extraction coordinator"
    );
}

#[test]
fn direct_public_reads_inherit_the_schema_publication_gate_from_open_operation() {
    let database = include_str!("../../src/database.rs");

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
    let database = include_str!("../../src/database/maintenance_and_limits.rs");
    let client = include_str!("../../src/sync_client.rs");
    let server = include_str!("../../src/sync_server.rs");

    assert!(
        database.contains("pub(crate) fn enter_outbound_sync_schema_read(&self)")
            && database.contains("_inner: self.enter_schema_publication_gate(false)"),
        "authenticated sync orchestration must reuse the existing schema-publication read side"
    );

    let client_enter = client
        .find("let schema_read = self.db.enter_outbound_sync_schema_read();")
        .expect("push enters outbound schema lease");
    let client_extract = client[client_enter..]
        .find("checked_outbound_changes_since_with_arrivals(")
        .map(|offset| client_enter + offset)
        .expect("push extracts under lease");
    let client_provenance = client[client_extract..]
        .find("self.db.outbound_ddl_provenance(\n                    &batch,")
        .map(|offset| client_extract + offset)
        .expect("push prepares DDL provenance under lease");
    let client_drop = client[client_provenance..]
        .find("drop(schema_read);")
        .map(|offset| client_provenance + offset)
        .expect("push drops lease");
    let client_await = client[client_drop..]
        .find(".request_push(encoded, protocol_version, retry_safety, unit_memory.clone())")
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
        .find("db.checked_changes_since_with_arrivals(")
        .map(|offset| server_enter + offset)
        .expect("pull extracts under lease");
    let server_provenance = server[server_extract..]
        .find("db.outbound_ddl_provenance(\n                    &unit,")
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
    let database = include_str!("../../src/database.rs");
    let apply = database
        .split_once("fn apply_changes_impl(")
        .map(|(_, body)| body)
        .expect("received apply implementation");

    let enter = apply
        .find("let entered_with_schema_write = requires_schema_write(&changes);")
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
        .find("if !entered_with_schema_write && requires_schema_write(&changes)")
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
