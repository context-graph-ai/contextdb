//! Independent witnesses for the real private predicate-copy producer and cursor.
//! These tests deliberately use the existing allocator oracle, not its byte quote.
use super::*;
use crate::memory_accounting::MemoryAccountant;
use crate::persistence::content_identity::proof_allocation_oracle_tests as oracle;
use contextdb_core::read_contract::DeadlineWait;
use contextdb_core::{AclRef, MaintainedCounts, RankPolicy, ScopeLabelKind, SortDirection};
use std::sync::atomic::Ordering;

struct Clock;
impl DeadlineClock for Clock {
    fn now_ms(&self) -> u64 {
        0
    }
    fn wait_until(&self, _: u64) -> DeadlineWait<'_> {
        Box::pin(async {})
    }
}
fn limits() -> ReadLimits {
    ReadLimits {
        result_rows: 256,
        result_bytes: 16 << 20,
        work: 10_000_000,
        active_ms: 600_000,
        memory: 64 << 20,
        cursor_page_rows: 1,
        cursor_page_bytes: 4 << 20,
        cursor_idle_ms: 600_000,
        cursor_lifetime_ms: 600_000,
    }
}
fn context(owner: Arc<MemoryAccountant>) -> ControlArc<RequestContext> {
    let context = RequestContext::new(
        owner,
        limits(),
        ReadResumption::OneShot,
        Arc::new(Clock),
        OwnerReadCancellation::new(),
        None,
        None,
    )
    .unwrap();
    // Warm only the existing telemetry map entry. Its allocation is outside
    // the producer measurement; neither payload nor invoice survives this call.
    drop(
        context
            .admit_live_charge(BoundedWorkSource::VectorCandidates, 0, "warm_test_observer")
            .unwrap(),
    );
    context
}
fn wide_fixture() -> (Expr, TableMeta) {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE copy_rows (id INTEGER PRIMARY KEY, label TEXT, embedding VECTOR(2))",
        &HashMap::new(),
    )
    .unwrap();
    let mut meta = db.table_meta("copy_rows").unwrap();
    let column = &mut meta.columns[1];
    column.acl_ref = Some(AclRef {
        ref_table: "a".repeat(2053),
        ref_column: "b".repeat(2063),
    });
    column.scope_label = Some(ScopeLabelKind::Split {
        read_labels: vec!["r".repeat(2081)],
        write_labels: vec!["w".repeat(2083)],
    });
    column.rank_policy = Some(RankPolicy {
        joined_table: "j".repeat(2087),
        joined_column: "c".repeat(2089),
        anchor_column: "a".repeat(2099),
        sort_key: "s".repeat(2111),
        formula: "f".repeat(2113),
        protected_index: "p".repeat(2129),
    });
    let mut transitions = HashMap::with_capacity(128);
    for i in 0..96 {
        transitions.insert(format!("deleted-{i}"), vec!["gone".into()]);
    }
    for i in 0..96 {
        assert!(transitions.remove(&format!("deleted-{i}")).is_some());
    }
    transitions.insert("s".repeat(2131), vec!["t".repeat(2137)]);
    assert!(transitions.capacity() > transitions.len());
    meta.state_machine = Some(contextdb_core::StateMachineConstraint {
        column: "label".into(),
        transitions,
    });
    meta.delivery_manifest_tables = Some(vec!["d".repeat(2141), "e".repeat(2143)]);
    meta.maintained_counts = Some(MaintainedCounts::Lists(vec![vec![(
        "m".repeat(2153),
        SortDirection::Asc,
    )]]));
    let filter = Expr::BinaryOp {
        left: Box::new(Expr::Literal(contextdb_parser::ast::Literal::Text(
            "q".repeat(8191),
        ))),
        op: contextdb_parser::ast::BinOp::Eq,
        right: Box::new(Expr::Literal(contextdb_parser::ast::Literal::Text(
            "q".repeat(8191),
        ))),
    };
    (filter, meta)
}

// Acceptance: preadmit every native allocation; no payload on insufficient
// allowance; independent copies retain independent invoices through physical free.
// Decisive mutants: remove admission, omit manifest/count/native-map bytes, or
// refund the invoice before dropping the actual copied owner.
#[test]
#[serial_test::serial]
fn predicate_metadata_copy_is_preadmitted_and_funded_through_every_native_free() {
    let (filter, meta) = wide_fixture();
    let owner = Arc::new(MemoryAccountant::with_budget(64 << 20));
    let context = context(owner.clone());
    let baseline = owner.usage().used;
    let counter = Arc::as_ptr(&context.live_source_pages) as usize;
    assert_eq!(context.live_source_pages.load(Ordering::SeqCst), 0);
    // Borrowing either source, including inspecting it to quote the copy,
    // creates no owned payload and cannot debit the operation.
    let planned = vector_row_filter_copy_bytes(&filter, Some(&meta)).unwrap();
    assert_eq!(owner.usage().used, baseline);
    owner
        .set_budget(Some(baseline.checked_add(planned).unwrap() - 1))
        .unwrap();
    let (refused, requests, count) = oracle::source_requests_for_test(counter, || {
        copy_vector_row_filter(&filter, Some(&meta), &context)
    });
    assert!(
        matches!(&refused,
        Err(BoundedExecutionError::Engine(Error::MemoryBudgetExceeded {
            operation, requested_bytes, available_bytes, ..
        })) if operation == "vector_row_filter_copy" && *requested_bytes == planned && *available_bytes == planned - 1),
        "one byte short refuses with the concrete producer's typed memory error"
    );
    assert!(
        count < requests.len(),
        "the refusal observation must be complete"
    );
    let box_layout = std::alloc::Layout::new::<(Expr, Option<TableMeta>)>();
    assert!(
        !requests[..count]
            .iter()
            .any(|r| (r.1, r.2) == (box_layout.size(), box_layout.align())),
        "refusal precedes allocating the actual predicate/metadata box"
    );
    assert!(
        !requests[..count].iter().any(|r| r.1 == 8191),
        "no wide AST string is copied before refusal"
    );
    drop(refused);
    assert_eq!(owner.usage().used, baseline);
    assert_eq!(context.live_source_pages.load(Ordering::SeqCst), 0);
    owner.set_budget(Some(64 << 20)).unwrap();

    let (first, requests, count) = oracle::source_requests_for_test(counter, || {
        copy_vector_row_filter(&filter, Some(&meta), &context).unwrap()
    });
    assert!(
        count > 0 && count <= 128,
        "the physical free census must contain every allocation"
    );
    let bytes: usize = requests[..count].iter().map(|r| r.1).sum();
    assert_eq!(
        oracle::source_unfunded_peak_for_test().0,
        0,
        "zero unfunded bytes before any real allocation"
    );
    assert_eq!(
        first.1.bytes, bytes,
        "the invoice equals native requests, including the real map control bytes"
    );
    assert_eq!(first.1._credit.bytes(), bytes);
    assert_eq!(owner.usage().used, baseline + bytes);
    assert_eq!(context.live_source_pages.load(Ordering::SeqCst), bytes);
    let copied = first.0.1.as_ref().unwrap();
    let source_map = &meta.state_machine.as_ref().unwrap().transitions;
    let copied_map = &copied.state_machine.as_ref().unwrap().transitions;
    assert_eq!(copied_map, source_map);
    assert!(
        copied_map.capacity() < source_map.capacity(),
        "deleted/spare source buckets are not cloned"
    );
    assert_eq!(
        copied.delivery_manifest_tables,
        meta.delivery_manifest_tables
    );
    assert_eq!(copied.maintained_counts, meta.maintained_counts);
    assert_eq!(copied.columns, meta.columns);
    assert_ne!(
        copied.columns[1].name.as_ptr(),
        meta.columns[1].name.as_ptr()
    );
    assert!(
        requests[..count]
            .iter()
            .any(|r| r.0 == first.0.as_ref() as *const _ as usize && r.1 == box_layout.size())
    );
    let native =
        crate::memory_accounting::native_map_plan::<String, Vec<String>>(copied_map.len()).unwrap();
    assert_eq!(copied_map.capacity(), native.capacity);
    assert!(
        requests[..count]
            .iter()
            .any(|r| r.1 == native.layout.size() && r.2 == native.layout.align())
    );

    let second = copy_vector_row_filter(&filter, Some(&meta), &context).unwrap();
    let second_bytes = second.1.bytes;
    assert_ne!(first.0.as_ref() as *const _, second.0.as_ref() as *const _);
    assert_eq!(
        context.live_source_pages.load(Ordering::SeqCst),
        bytes + second_bytes
    );
    assert_eq!(owner.usage().used, baseline + bytes + second_bytes);
    // Select every allocation made by the first actual producer, not merely
    // a matching layout. The source counter stays alive through all frees.
    oracle::watch_source_free_for_test(&requests[..count], counter);
    drop(first);
    let freed = oracle::source_free_for_test(count);
    oracle::disarm_source_free_watch();
    for &(before, after, frees) in &freed[..count] {
        assert_eq!(
            frees, 1,
            "each real predicate/metadata allocation frees exactly once"
        );
        assert_eq!(
            before,
            bytes + second_bytes,
            "both invoices remain until the first payload's last physical free"
        );
        assert_eq!(
            after, before,
            "System.dealloc returns before the invoice is refunded"
        );
    }
    assert_eq!(
        context.live_source_pages.load(Ordering::SeqCst),
        second_bytes
    );
    assert_eq!(owner.usage().used, baseline + second_bytes);
    drop(second);
    assert_eq!(owner.usage().used, baseline);
    assert_eq!(context.live_source_pages.load(Ordering::SeqCst), 0);
    owner.set_budget(Some(baseline + bytes - 1)).unwrap();
    let (short, short_requests, short_count) = oracle::source_requests_for_test(counter, || {
        copy_vector_row_filter(&filter, Some(&meta), &context)
    });
    assert!(
        matches!(&short,
        Err(BoundedExecutionError::Engine(Error::MemoryBudgetExceeded {
            operation, requested_bytes, available_bytes, ..
        })) if operation == "vector_row_filter_copy" && *requested_bytes == planned && *available_bytes == bytes - 1),
        "one byte below independently measured physical copy bytes refuses with its typed producer error"
    );
    assert!(short_count < short_requests.len());
    assert!(
        !short_requests[..short_count]
            .iter()
            .any(|r| r.1 == 8191 || (r.1, r.2) == (box_layout.size(), box_layout.align())),
        "the concretely insufficient allowance refuses before AST strings or owner box exist"
    );
    drop(short);
    assert_eq!(owner.usage().used, baseline);
    assert_eq!(context.live_source_pages.load(Ordering::SeqCst), 0);
    // Valid owned containers cannot represent an overflowing native length;
    // exercise the actual native layout planner's rejection instead of forged Vecs.
    assert!(crate::memory_accounting::native_map_plan::<String, Vec<String>>(usize::MAX).is_err());
    drop(context);
    assert_eq!(owner.usage().used, 0);
}

fn vector_state(node: &PullNode) -> &VectorState {
    match &node.kind {
        PullKind::Vector(state) => state,
        PullKind::Project(state) => vector_state(&state.input),
        PullKind::Limit(state) => vector_state(&state.input),
        other => {
            let _ = other;
            panic!("fixture must reach the actual vector cursor")
        }
    }
}
fn retained_filter(
    cursor: &BoundedCursorHandle,
) -> (usize, usize, ControlArc<std::sync::atomic::AtomicUsize>) {
    let lifecycle = cursor_lifecycle(cursor);
    let BoundedCursorLifecycle::Open(state) = &*lifecycle else {
        panic!("fixture requires live continuation")
    };
    let vector = vector_state(&state.root);
    let (payload, charge) = vector
        .row_filter
        .as_ref()
        .expect("the actual route retains a copied row filter");
    assert!(charge.bytes > 0);
    assert_eq!(charge._credit.bytes(), charge.bytes);
    assert!(state.context.live_source_pages.load(Ordering::SeqCst) >= charge.bytes);
    (
        payload.as_ref() as *const _ as usize,
        charge.bytes,
        state.context.live_source_pages.clone(),
    )
}
fn ids(rows: &[Vec<Value>]) -> Vec<i64> {
    rows.iter()
        .map(|r| match r.as_slice() {
            [Value::Int64(id)] => *id,
            other => panic!("unexpected row {other:?}"),
        })
        .collect()
}
fn plain_fixture(path: &std::path::Path, indexed: bool) -> Arc<Database> {
    let db = Database::open_with_options(
        path,
        crate::DatabaseOpenOptions {
            maintenance_policy: crate::MaintenancePolicy::CallerDriven,
            ..Default::default()
        },
    )
    .unwrap();
    db.execute("CREATE TABLE held_filter (id INTEGER PRIMARY KEY, label TEXT, bucket INTEGER NOT NULL, embedding VECTOR(2) PARTITION_KEY (bucket) SEARCH_MODE AUTO HNSW (M = 2, EF_SEARCH = 8))", &HashMap::new()).unwrap();
    for id in 0..64_i64 {
        db.execute(
            "INSERT INTO held_filter VALUES ($id, 'wanted', 1, $vector)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("vector".into(), Value::Vector(vec![1.0, id as f32 / 32.0])),
            ]),
        )
        .unwrap();
    }
    // A closer row with the wrong predicate is a meaningful negative.
    db.execute(
        "INSERT INTO held_filter VALUES (99, 'hidden', 1, [1.0, 0.0])",
        &HashMap::new(),
    )
    .unwrap();
    if indexed {
        db.execute(
            "CREATE INDEX held_label ON held_filter(label)",
            &HashMap::new(),
        )
        .unwrap();
        for _ in 0..32 {
            db.run_maintenance_cycle().unwrap();
        }
    }
    Arc::new(db)
}

// Acceptance: the actual AUTO exhaustive and INDEXED graph row-check routes
// keep their predicate across settlement, close, cancellation, and cursor drop.
// Decisive mutants: remove row_filter's charge or settle/refund it at first-page return.
#[test]
#[serial_test::serial]
fn a_real_vector_cursor_keeps_its_filter_invoice_across_pages_and_terminal_cleanup() {
    for indexed in [false, true] {
        for ending in ["close", "cancel", "drop"] {
            let dir = tempfile::tempdir().unwrap();
            let db = plain_fixture(&dir.path().join("filter.redb"), indexed);
            let owner = Arc::new(MemoryAccountant::with_budget(64 << 20));
            let mode = if indexed { "INDEXED" } else { "AUTO" };
            let sql = format!(
                "SELECT id FROM held_filter WHERE label = 'wanted' ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT 8"
            );
            let params = HashMap::from([("query".into(), Value::Vector(vec![1.0, 0.0]))]);
            let mut opened = open_cursor(
                db.clone() as Arc<dyn ReadExecutionTarget>,
                &sql,
                &params,
                owner.clone(),
                limits(),
                Arc::new(Clock),
                OwnerReadCancellation::new(),
                None,
            )
            .unwrap();
            let first = opened.first_page.into_public();
            assert_eq!(ids(&first.rows), vec![0]);
            assert!(first.has_more);
            let (pointer, invoice, live) = retained_filter(&opened.cursor);
            {
                let lifecycle = cursor_lifecycle(&opened.cursor);
                let BoundedCursorLifecycle::Open(state) = &*lifecycle else {
                    unreachable!()
                };
                let vector = vector_state(&state.root);
                assert!(
                    vector.row_checked,
                    "the real search must install the row-filter callback"
                );
                if indexed {
                    assert!(
                        db.__debug_last_query_vector_used_hnsw_for_test(),
                        "the indexed fixture must reach the graph callback"
                    );
                    assert!(
                        vector.row_filter.as_ref().unwrap().0.1.is_some(),
                        "the graph callback owns its copied authorization metadata"
                    );
                    assert!(
                        vector.row_checks.get() > vector.row_check_allowance.unwrap() / 2,
                        "the callback must inspect rows beyond the bounded candidate probe"
                    );
                } else {
                    assert!(
                        vector.row_filter.as_ref().unwrap().0.1.is_none(),
                        "the unrestricted AUTO fallback borrows no authorization copy"
                    );
                    assert!(
                        vector.row_checks.get() > 0,
                        "the exhaustive route must actually invoke its row filter"
                    );
                }
            }
            oracle::watch_registry_free_for_test(pointer, &*live as *const _ as usize);
            let fetched = fetch_cursor(
                &mut opened.cursor,
                NonZeroUsize::new(1),
                OwnerReadCancellation::new(),
                None,
            )
            .unwrap();
            let second = fetched.page.into_public();
            assert_eq!(ids(&second.rows), vec![1]);
            assert!(second.has_more);
            let (later_pointer, later_invoice, later_live) = retained_filter(&opened.cursor);
            assert_eq!((later_pointer, later_invoice), (pointer, invoice));
            drop(later_live);
            assert!(
                !oracle::registry_free_observation_for_test(false).2,
                "settlement must not free the retained box"
            );
            drop(fetched.observation);
            match ending {
                "close" => close_cursor(&mut opened.cursor).unwrap(),
                "cancel" => {
                    let cancel = OwnerReadCancellation::new();
                    cancel.cancel();
                    assert!(matches!(
                        fetch_cursor(&mut opened.cursor, NonZeroUsize::new(1), cancel, None),
                        Err(BoundedExecutionError::Cancelled)
                    ));
                    assert_eq!(
                        retained_filter(&opened.cursor).0,
                        pointer,
                        "cancellation preserves the continuation until actual close/drop"
                    );
                }
                "drop" => {}
                _ => unreachable!(),
            }
            drop(opened.cursor);
            let (before, after, freed) = oracle::registry_free_observation_for_test(true);
            assert!(
                freed,
                "terminal cleanup physically frees the actual retained box"
            );
            assert!(
                before >= invoice,
                "the invoice survives through its physical free"
            );
            assert_eq!(before, after);
            drop(live);
            drop(opened.observation);
            db.__reset_last_query_vector_trace_for_test();
            // These internal helper receipts still own SourceDecodeBudget,
            // which retains the request context until publication finishes.
            // Public pages above have already crossed their ownership boundary.
            drop(fetched.publication);
            drop(opened.publication);
            assert_eq!(
                owner.usage().used,
                0,
                "terminal cleanup returns all request-owned bytes"
            );
            assert_eq!(
                ids(&first.rows),
                vec![0],
                "public pages survive later cleanup"
            );
            assert_eq!(ids(&second.rows), vec![1]);
        }
    }
}

// Acceptance: EXACT/AUTO/INDEXED preserve authorized, ordered membership on
// each captured snapshot, with later grants/writes invisible to open cursors.
// A completed public page remains application-owned across the later operation.
// Decisive mutants: bypass labels/grants/predicate, or substitute current rows
// or grants when fetching an existing cursor. This is semantic route coverage;
// it does not claim that the ACL directory route owns row_filter metadata.
#[test]
#[serial_test::serial]
fn authorized_vector_modes_keep_their_captured_rows_and_grants_after_later_writes() {
    use contextdb_core::{Principal, ScopeLabel};
    use std::collections::BTreeSet;
    use uuid::Uuid;
    let dir = tempfile::tempdir().unwrap();
    let admin = Database::open_with_options(
        dir.path().join("authorized.redb"),
        crate::DatabaseOpenOptions {
            maintenance_policy: crate::MaintenancePolicy::CallerDriven,
            ..Default::default()
        },
    )
    .unwrap();
    admin.execute("CREATE TABLE acl_grants (id UUID PRIMARY KEY, principal_kind TEXT, principal_id TEXT, acl_id UUID) SYNC OFF", &HashMap::new()).unwrap();
    admin.execute("CREATE TABLE authorized_rows (id INTEGER PRIMARY KEY, bucket INTEGER NOT NULL, label TEXT, scope TEXT SCOPE_LABEL_READ ('visible','hidden') WRITE ('visible','hidden'), acl_id UUID ACL REFERENCES acl_grants(acl_id), embedding VECTOR(2) PARTITION_KEY (bucket) SEARCH_MODE AUTO) SYNC OFF", &HashMap::new()).unwrap();
    admin
        .execute(
            "CREATE INDEX authorized_candidates ON authorized_rows(bucket,label)",
            &HashMap::new(),
        )
        .unwrap();
    admin
        .execute(
            "INSERT INTO acl_grants VALUES ($id, 'Human', 'reader', $acl)",
            &HashMap::from([
                ("id".into(), Value::Uuid(Uuid::from_u128(1))),
                ("acl".into(), Value::Uuid(Uuid::from_u128(7))),
            ]),
        )
        .unwrap();
    // Every forbidden row is at least as close as an allowed row. Removing a
    // filter, scope-label check, or grant check therefore changes the top three.
    for (id, label, scope, acl, distance) in [
        (0, "wanted", "visible", 7, 0.1),
        (1, "wanted", "visible", 7, 0.2),
        (2, "wanted", "visible", 7, 0.3),
        (90, "other", "visible", 7, 0.0),
        (91, "wanted", "hidden", 7, 0.0),
        (92, "wanted", "visible", 8, 0.0),
    ] {
        admin
            .execute(
                "INSERT INTO authorized_rows VALUES ($id, 1, $label, $scope, $acl, $vector)",
                &HashMap::from([
                    ("id".into(), Value::Int64(id)),
                    ("label".into(), Value::Text(label.into())),
                    ("scope".into(), Value::Text(scope.into())),
                    ("acl".into(), Value::Uuid(Uuid::from_u128(acl))),
                    ("vector".into(), Value::Vector(vec![1.0, distance])),
                ]),
            )
            .unwrap();
    }
    // Build the actual maintained graph through the public maintenance path;
    // INDEXED below must refuse rather than pass if no graph is ready.
    for _ in 0..32 {
        admin.run_maintenance_cycle().unwrap();
    }
    let reader = Arc::new(admin.scoped_with_constraints(
        None,
        Some(BTreeSet::from([ScopeLabel::new("visible")])),
        Some(Principal::Human("reader".into())),
    ));
    let owner = Arc::new(MemoryAccountant::with_budget(64 << 20));
    let params = HashMap::from([("query".into(), Value::Vector(vec![1.0, 0.0]))]);
    let mut held = Vec::new();
    for mode in ["EXACT", "AUTO", "INDEXED"] {
        let sql = format!(
            "SELECT id FROM authorized_rows WHERE bucket = 1 AND label = 'wanted' ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT 3"
        );
        let opened = open_cursor(
            reader.clone() as Arc<dyn ReadExecutionTarget>,
            &sql,
            &params,
            owner.clone(),
            limits(),
            Arc::new(Clock),
            OwnerReadCancellation::new(),
            None,
        )
        .unwrap();
        let first = opened.first_page.into_public();
        assert_eq!(
            ids(&first.rows),
            vec![0],
            "{mode} enforces predicate, labels and grants"
        );
        assert!(first.has_more);
        if mode == "INDEXED" {
            assert!(
                reader.__debug_last_query_vector_used_hnsw_for_test(),
                "INDEXED uses the maintained graph"
            );
        }
        held.push((mode, sql, opened.cursor, opened.observation, first));
    }
    // Withdraw the original entitlement and grant the formerly forbidden ACL.
    // The old cursors must still use the original grants and original rows.
    admin
        .execute(
            "DELETE FROM acl_grants WHERE id = $id",
            &HashMap::from([("id".into(), Value::Uuid(Uuid::from_u128(1)))]),
        )
        .unwrap();
    admin
        .execute(
            "INSERT INTO acl_grants VALUES ($id, 'Human', 'reader', $acl)",
            &HashMap::from([
                ("id".into(), Value::Uuid(Uuid::from_u128(2))),
                ("acl".into(), Value::Uuid(Uuid::from_u128(8))),
            ]),
        )
        .unwrap();
    admin
        .execute(
            "INSERT INTO authorized_rows VALUES (93, 1, 'wanted', 'visible', $acl, [1.0, 0.01])",
            &HashMap::from([("acl".into(), Value::Uuid(Uuid::from_u128(8)))]),
        )
        .unwrap();
    for (mode, _, mut cursor, observation, first) in held {
        let mut rows = ids(&first.rows);
        let mut more = first.has_more;
        while more {
            let fetched = fetch_cursor(
                &mut cursor,
                NonZeroUsize::new(1),
                OwnerReadCancellation::new(),
                None,
            )
            .unwrap();
            let page = fetched.page.into_public();
            rows.extend(ids(&page.rows));
            more = page.has_more;
        }
        assert_eq!(
            rows,
            vec![0, 1, 2],
            "{mode} keeps captured rows and grants across the writer"
        );
        close_cursor(&mut cursor).unwrap();
        drop(cursor);
        drop(observation);
        assert_eq!(
            ids(&first.rows),
            vec![0],
            "public answer survives later writes and cursor release"
        );
    }
    // A fresh operation observes both the withdrawn grant and the newly
    // granted ACL; the negative control cannot pass by ignoring the writer.
    let sql = "SELECT id FROM authorized_rows WHERE bucket = 1 AND label = 'wanted' ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 3";
    let fresh = execute(
        reader.as_ref(),
        sql,
        &params,
        owner.clone(),
        limits(),
        Arc::new(Clock),
        OwnerReadCancellation::new(),
        None,
    )
    .unwrap();
    let public = fresh.result.into_public();
    assert_eq!(ids(&public.rows), vec![92, 93]);
    reader.__reset_last_query_vector_trace_for_test();
    admin.__reset_last_query_vector_trace_for_test();
    // execute is an internal helper: converting its answer does not finish
    // publication or destroy the publication scope's retained request context.
    drop(fresh.publication);
    assert_eq!(owner.usage().used, 0);
    assert_eq!(
        ids(&public.rows),
        vec![92, 93],
        "the final public answer carries no cross-operation invoice"
    );
}
