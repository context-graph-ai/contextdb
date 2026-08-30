use contextdb_core::read_contract::{
    CursorPage, OwnerReadCancellation, ReadFailureDetail, ReadFailureLimit, ReadLimits, ReadRoute,
};
use contextdb_core::{ContextId, Error, Principal, ScopeLabel, Value};
use contextdb_engine::database::{ReadExecutionConvergenceEvent, ReadExecutionConvergenceObserver};
use contextdb_engine::read_session::{
    ReadKernelCancellationEvent, ReadKernelSource, ReadKernelSourceEvent, ReadKernelTestObserver,
    ReadSessionOperation,
};
use contextdb_engine::{
    Database, DatabasePlugin, QueryResult, ReadCursor, ReadSession, ReadSessionOptions,
};
use std::collections::{BTreeSet, HashMap};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

fn under_limit_row_count() -> usize {
    let limits = ReadLimits::default();
    usize::try_from(
        limits
            .cursor_page_rows
            .min(limits.result_rows.saturating_sub(1))
            .max(1),
    )
    .expect("declared row policy fits this platform")
}

fn over_limit_row_count() -> usize {
    usize::try_from(ReadLimits::default().result_rows.saturating_add(1))
        .expect("declared result-row policy fits this platform")
}

fn seed_rows(database: &Database, table: &str, row_count: usize) {
    database
        .execute(
            &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, payload TEXT)"),
            &HashMap::new(),
        )
        .expect("create the dynamic bounded-read fixture");
    for id in 0..row_count {
        database
            .execute(
                &format!("INSERT INTO {table} (id, payload) VALUES ($id, $payload)"),
                &HashMap::from([
                    ("id".to_owned(), Value::Int64(i64::try_from(id).unwrap())),
                    (
                        "payload".to_owned(),
                        Value::Text(format!("payload-{id}-{row_count}")),
                    ),
                ]),
            )
            .expect("insert the dynamic bounded-read fixture row");
    }
}

fn cursor_page_ids(page: &CursorPage) -> Vec<i64> {
    page.rows
        .iter()
        .map(|row| match row.as_slice() {
            [Value::Int64(id)] => *id,
            other => panic!("cursor id projection returned {other:?}"),
        })
        .collect()
}

struct ApplicationFixture {
    admin: Database,
    allowed_context: uuid::Uuid,
    allowed_scope: ScopeLabel,
    principal: Principal,
    visible_rows: usize,
    hidden_by_context: String,
    hidden_by_scope: String,
    hidden_by_principal: String,
}

fn application_fixture() -> ApplicationFixture {
    let admin = Database::open_memory();
    let limits = ReadLimits::default();
    let identity_seed = u128::from(limits.result_rows)
        .saturating_mul(1_000_003)
        .saturating_add(u128::from(limits.result_bytes));
    let allowed_context = uuid::Uuid::from_u128(identity_seed.saturating_add(1));
    let forbidden_context = uuid::Uuid::from_u128(identity_seed.saturating_add(2));
    let visible_acl = uuid::Uuid::from_u128(identity_seed.saturating_add(3));
    let forbidden_acl = uuid::Uuid::from_u128(identity_seed.saturating_add(4));
    let principal_name = format!("application-reader-{}", limits.work);
    let principal = Principal::Agent(principal_name.clone());
    let allowed_scope_name = format!("visible-{}", limits.cursor_page_rows);
    let forbidden_scope_name = format!("forbidden-{}", limits.cursor_page_rows);
    let allowed_scope = ScopeLabel::new(allowed_scope_name.clone());
    let visible_rows = under_limit_row_count();

    admin
        .execute(
            "CREATE TABLE acl_grants (id UUID PRIMARY KEY, principal_kind TEXT, principal_id TEXT, acl_id UUID)",
            &HashMap::new(),
        )
        .expect("the administrative owner creates the ACL table");
    admin
        .execute(
            &format!(
                "CREATE TABLE application_rows (id INTEGER PRIMARY KEY, context_id UUID CONTEXT_ID, scope TEXT SCOPE_LABEL_READ ('{allowed_scope_name}','{forbidden_scope_name}') WRITE ('{allowed_scope_name}','{forbidden_scope_name}'), acl_id UUID ACL REFERENCES acl_grants(acl_id), payload TEXT)"
            ),
            &HashMap::new(),
        )
        .expect("the administrative owner creates the constrained data table");
    admin
        .execute(
            "INSERT INTO acl_grants (id, principal_kind, principal_id, acl_id) VALUES ($id, 'Agent', $principal, $acl)",
            &HashMap::from([
                ("id".to_owned(), Value::Uuid(uuid::Uuid::from_u128(identity_seed.saturating_add(5)))),
                ("principal".to_owned(), Value::Text(principal_name)),
                ("acl".to_owned(), Value::Uuid(visible_acl)),
            ]),
        )
        .expect("the administrative owner grants the visible ACL");

    for id in 0..visible_rows {
        admin
            .execute(
                "INSERT INTO application_rows (id, context_id, scope, acl_id, payload) VALUES ($id, $context, $scope, $acl, $payload)",
                &HashMap::from([
                    ("id".to_owned(), Value::Int64(i64::try_from(id).unwrap())),
                    ("context".to_owned(), Value::Uuid(allowed_context)),
                    ("scope".to_owned(), Value::Text(allowed_scope_name.clone())),
                    ("acl".to_owned(), Value::Uuid(visible_acl)),
                    (
                        "payload".to_owned(),
                        Value::Text(format!("visible-{id}-{visible_rows}")),
                    ),
                ]),
            )
            .expect("the administrative owner inserts a visible row");
    }

    let hidden_by_context = format!("hidden-context-{visible_rows}");
    let hidden_by_scope = format!("hidden-scope-{visible_rows}");
    let hidden_by_principal = format!("hidden-principal-{visible_rows}");
    for (offset, context, scope, acl, payload) in [
        (
            0usize,
            forbidden_context,
            allowed_scope_name.clone(),
            visible_acl,
            hidden_by_context.clone(),
        ),
        (
            1,
            allowed_context,
            forbidden_scope_name,
            visible_acl,
            hidden_by_scope.clone(),
        ),
        (
            2,
            allowed_context,
            allowed_scope_name,
            forbidden_acl,
            hidden_by_principal.clone(),
        ),
    ] {
        admin
            .execute(
                "INSERT INTO application_rows (id, context_id, scope, acl_id, payload) VALUES ($id, $context, $scope, $acl, $payload)",
                &HashMap::from([
                    (
                        "id".to_owned(),
                        Value::Int64(i64::try_from(visible_rows.saturating_add(offset)).unwrap()),
                    ),
                    ("context".to_owned(), Value::Uuid(context)),
                    ("scope".to_owned(), Value::Text(scope)),
                    ("acl".to_owned(), Value::Uuid(acl)),
                    ("payload".to_owned(), Value::Text(payload)),
                ]),
            )
            .expect("the administrative owner inserts an independently forbidden row");
    }

    ApplicationFixture {
        admin,
        allowed_context,
        allowed_scope,
        principal,
        visible_rows,
        hidden_by_context,
        hidden_by_scope,
        hidden_by_principal,
    }
}

fn payloads(result: QueryResult) -> BTreeSet<String> {
    result
        .rows
        .into_iter()
        .map(|row| match row.as_slice() {
            [Value::Text(payload)] => payload.clone(),
            other => panic!("payload projection returned {other:?}"),
        })
        .collect()
}

fn session_payloads(database: &Database) -> BTreeSet<String> {
    database
        .read_session(ReadLimits::default())
        .expect("derive a bounded live-state session from the constrained handle")
        .execute(
            "SELECT payload FROM application_rows ORDER BY id",
            &HashMap::new(),
        )
        .map(payloads)
        .expect("the bounded live-state query completes")
}

#[test]
fn under_limit_application_query_runs_cross_thread_on_the_derived_owner_view() {
    let fixture = application_fixture();
    let constrained = fixture.admin.scoped_with_constraints(
        Some(BTreeSet::from([ContextId::new(fixture.allowed_context)])),
        Some(BTreeSet::from([fixture.allowed_scope.clone()])),
        Some(fixture.principal.clone()),
    );
    let session = constrained
        .read_session(ReadLimits::default())
        .expect("derive the bounded application session after administrative setup");
    assert_eq!(session.route(), ReadRoute::Owner);

    let result = std::thread::scope(|scope| {
        scope
            .spawn(|| {
                session.execute(
                    "SELECT payload FROM application_rows ORDER BY id",
                    &HashMap::new(),
                )
            })
            .join()
            .expect("the application read thread completes")
    })
    .expect("the bounded kernel serves the scoped live view directly");
    assert_eq!(result.rows.len(), fixture.visible_rows);
}

#[test]
fn an_inherited_context_alone_is_refused_an_access_controlled_table() {
    let fixture = application_fixture();
    let constrained = fixture.admin.scoped_with_constraints(
        Some(BTreeSet::from([ContextId::new(fixture.allowed_context)])),
        None,
        None,
    );
    let refusal = constrained
        .read_session(ReadLimits::default())
        .expect("derive a bounded live-state session from the constrained handle")
        .execute(
            "SELECT payload FROM application_rows ORDER BY id",
            &HashMap::new(),
        )
        .expect_err("an access-controlled table is not served to a handle without a principal");
    assert!(
        matches!(&refusal, Error::PrincipalRequired { table } if table == "application_rows"),
        "the context-only handle reported {refusal:?}"
    );
}

#[test]
fn an_inherited_scope_alone_is_refused_an_access_controlled_table() {
    let fixture = application_fixture();
    let constrained = fixture.admin.scoped_with_constraints(
        None,
        Some(BTreeSet::from([fixture.allowed_scope.clone()])),
        None,
    );
    let refusal = constrained
        .read_session(ReadLimits::default())
        .expect("derive a bounded live-state session from the constrained handle")
        .execute(
            "SELECT payload FROM application_rows ORDER BY id",
            &HashMap::new(),
        )
        .expect_err("an access-controlled table is not served to a handle without a principal");
    assert!(
        matches!(&refusal, Error::PrincipalRequired { table } if table == "application_rows"),
        "the scope-only handle reported {refusal:?}"
    );
}

#[test]
fn inherited_principal_hides_only_the_row_without_its_acl_grant() {
    let fixture = application_fixture();
    let constrained =
        fixture
            .admin
            .scoped_with_constraints(None, None, Some(fixture.principal.clone()));
    let visible = session_payloads(&constrained);
    assert!(visible.contains(&fixture.hidden_by_context));
    assert!(visible.contains(&fixture.hidden_by_scope));
    assert!(!visible.contains(&fixture.hidden_by_principal));
}

#[test]
fn an_inherited_context_and_principal_hide_the_foreign_context_and_ungranted_rows() {
    let fixture = application_fixture();
    let constrained = fixture.admin.scoped_with_constraints(
        Some(BTreeSet::from([ContextId::new(fixture.allowed_context)])),
        None,
        Some(fixture.principal.clone()),
    );
    let visible = session_payloads(&constrained);
    assert!(!visible.contains(&fixture.hidden_by_context));
    assert!(visible.contains(&fixture.hidden_by_scope));
    assert!(!visible.contains(&fixture.hidden_by_principal));
}

#[test]
fn an_inherited_scope_and_principal_hide_the_forbidden_scope_and_ungranted_rows() {
    let fixture = application_fixture();
    let constrained = fixture.admin.scoped_with_constraints(
        None,
        Some(BTreeSet::from([fixture.allowed_scope.clone()])),
        Some(fixture.principal.clone()),
    );
    let visible = session_payloads(&constrained);
    assert!(visible.contains(&fixture.hidden_by_context));
    assert!(!visible.contains(&fixture.hidden_by_scope));
    assert!(!visible.contains(&fixture.hidden_by_principal));
}

#[derive(Default)]
struct ExecutionConvergenceObserver(Mutex<Vec<ReadExecutionConvergenceEvent>>);

impl ReadExecutionConvergenceObserver for ExecutionConvergenceObserver {
    fn observe(&self, event: ReadExecutionConvergenceEvent) {
        self.0
            .lock()
            .expect("execution convergence observation lock")
            .push(event);
    }
}

impl ExecutionConvergenceObserver {
    fn events(&self) -> Vec<ReadExecutionConvergenceEvent> {
        self.0
            .lock()
            .expect("execution convergence observation lock")
            .clone()
    }
}

#[test]
fn database_execute_plans_once_and_drains_the_shared_pull_kernel_once_without_a_cap() {
    let database = Database::open_memory();
    let row_count = over_limit_row_count();
    database
        .execute(
            "CREATE TABLE uncapped_rows (id INTEGER, payload TEXT)",
            &HashMap::new(),
        )
        .expect("create the uncapped scan fixture");
    for id in 0..row_count {
        database
            .execute(
                "INSERT INTO uncapped_rows (id, payload) VALUES ($id, $payload)",
                &HashMap::from([
                    ("id".to_owned(), Value::Int64(i64::try_from(id).unwrap())),
                    (
                        "payload".to_owned(),
                        Value::Text(format!("uncapped-{id}-{row_count}")),
                    ),
                ]),
            )
            .expect("insert the uncapped scan fixture row");
    }

    let observer = Arc::new(ExecutionConvergenceObserver::default());
    database.__reset_relational_scan_rows_touched();
    let uncapped = database
        .with_read_execution_convergence_observer_for_test(observer.clone(), || {
            database.execute(
                "SELECT id, payload FROM uncapped_rows WHERE id >= 0",
                &HashMap::new(),
            )
        })
        .expect("the established embedding API remains uncapped");
    assert_eq!(uncapped.rows.len(), row_count);
    assert_eq!(
        database.__relational_scan_rows_touched(),
        u64::try_from(row_count).unwrap(),
        "the result must come from one physical table scan, not a pull execution plus an eager execution",
    );

    let mut expected = vec![
        ReadExecutionConvergenceEvent::PublicExecuteEntered,
        ReadExecutionConvergenceEvent::PlanReady,
        ReadExecutionConvergenceEvent::PullKernelEntered,
    ];
    expected.extend((0..row_count).map(|completed_items| {
        ReadExecutionConvergenceEvent::PullKernelSourceTouch {
            source: ReadKernelSource::TableRow,
            completed_items: u64::try_from(completed_items).unwrap(),
        }
    }));
    expected.push(ReadExecutionConvergenceEvent::PullKernelDrained);
    assert_eq!(
        observer.events(),
        expected,
        "planning, the real table source loop, and terminal drain must form one production execution",
    );
}

#[test]
fn legacy_execute_preserves_the_exact_parse_error_before_planning_or_pull() {
    let database = Database::open_memory();
    let observer = Arc::new(ExecutionConvergenceObserver::default());
    let table = format!("parse_error_rows_{}", ReadLimits::default().result_rows);
    let sql = format!("SELECT id FROM {table} GROUP BY id");

    let error = database
        .with_read_execution_convergence_observer_for_test(observer.clone(), || {
            database.execute(&sql, &HashMap::new())
        })
        .expect_err("the established parser refusal remains exact");
    assert!(matches!(
        error,
        Error::ParseError(message) if message == "GROUP BY is not supported"
    ));
    assert_eq!(
        observer.events(),
        vec![ReadExecutionConvergenceEvent::PublicExecuteEntered],
        "parse refusal must happen before planning or pull execution",
    );
}

#[test]
fn legacy_execute_preserves_the_exact_static_planning_error_without_entering_pull() {
    let database = Database::open_memory();
    let table = format!("planning_error_rows_{}", under_limit_row_count());
    let missing_column = format!("missing_{}", over_limit_row_count());
    database
        .execute(
            &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY)"),
            &HashMap::new(),
        )
        .expect("create the static-planning refusal fixture");
    let observer = Arc::new(ExecutionConvergenceObserver::default());

    let error = database
        .with_read_execution_convergence_observer_for_test(observer.clone(), || {
            database.execute(
                &format!("SELECT {missing_column} FROM {table}"),
                &HashMap::new(),
            )
        })
        .expect_err("the established static plan refusal remains exact");
    assert!(matches!(
        error,
        Error::ColumnNotFound {
            table: error_table,
            column: error_column,
        } if error_table == table && error_column == missing_column
    ));
    assert_eq!(
        observer.events(),
        vec![
            ReadExecutionConvergenceEvent::PublicExecuteEntered,
            ReadExecutionConvergenceEvent::PlanReady,
        ],
        "a statically invalid plan must not enter the pull kernel",
    );
}

#[test]
fn legacy_execute_preserves_the_exact_execution_error_in_one_pull_execution() {
    let database = Database::open_memory();
    let table = format!("execution_error_rows_{}", under_limit_row_count());
    database
        .execute(
            &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, divisor INTEGER)"),
            &HashMap::new(),
        )
        .expect("create the execution refusal fixture");
    database
        .execute(
            &format!("INSERT INTO {table} (id, divisor) VALUES ($id, $divisor)"),
            &HashMap::from([
                (
                    "id".to_owned(),
                    Value::Int64(i64::try_from(under_limit_row_count()).unwrap()),
                ),
                ("divisor".to_owned(), Value::Int64(0)),
            ]),
        )
        .expect("insert the execution refusal row");
    let observer = Arc::new(ExecutionConvergenceObserver::default());
    database.__reset_relational_scan_rows_touched();

    let error = database
        .with_read_execution_convergence_observer_for_test(observer.clone(), || {
            database.execute(
                &format!("SELECT id / divisor FROM {table}"),
                &HashMap::new(),
            )
        })
        .expect_err("the established execution refusal remains exact");
    assert!(matches!(
        error,
        Error::PlanError(message) if message == "division by zero"
    ));
    assert_eq!(
        database.__relational_scan_rows_touched(),
        1,
        "the failing row must be scanned once rather than by an eager engine and a pull engine",
    );
    assert_eq!(
        observer.events(),
        vec![
            ReadExecutionConvergenceEvent::PublicExecuteEntered,
            ReadExecutionConvergenceEvent::PlanReady,
            ReadExecutionConvergenceEvent::PullKernelEntered,
            ReadExecutionConvergenceEvent::PullKernelSourceTouch {
                source: ReadKernelSource::TableRow,
                completed_items: 0,
            },
        ],
        "the exact execution error must emerge from the one planned pull without a success drain",
    );
}

struct RejectingQueryPlugin {
    hook: String,
    reason: String,
}

impl DatabasePlugin for RejectingQueryPlugin {
    fn on_query(&self, _sql: &str) -> contextdb_core::Result<()> {
        Err(Error::PluginRejected {
            hook: self.hook.clone(),
            reason: self.reason.clone(),
        })
    }
}

#[test]
fn legacy_execute_preserves_the_exact_plugin_error_before_planning_or_pull() {
    let limits = ReadLimits::default();
    let hook = format!("query-{}", limits.work);
    let reason = format!("refusal-{}", limits.result_bytes);
    let database = Database::open_memory_with_plugin(Arc::new(RejectingQueryPlugin {
        hook: hook.clone(),
        reason: reason.clone(),
    }))
    .expect("open the plugin refusal fixture");
    let observer = Arc::new(ExecutionConvergenceObserver::default());

    let error = database
        .with_read_execution_convergence_observer_for_test(observer.clone(), || {
            database.execute("SELECT * FROM plugin_refusal_rows", &HashMap::new())
        })
        .expect_err("the configured plugin refusal remains exact");
    assert!(matches!(
        error,
        Error::PluginRejected {
            hook: error_hook,
            reason: error_reason,
        } if error_hook == hook && error_reason == reason
    ));
    assert_eq!(
        observer.events(),
        vec![ReadExecutionConvergenceEvent::PublicExecuteEntered],
        "plugin rejection must retain its established position before planning and pull",
    );
}

#[test]
fn bounded_session_refuses_a_result_one_row_past_its_ceiling_naming_the_ceiling_without_publishing_partial_rows()
 {
    let database = Database::open_memory();
    let limits = ReadLimits::default();
    let row_count = over_limit_row_count();
    seed_rows(&database, "bounded_rows", row_count);

    let refusal = database
        .read_session(limits)
        .expect("construct the bounded session over the same dynamic result shape")
        .execute(
            "SELECT id, payload FROM bounded_rows ORDER BY id",
            &HashMap::new(),
        )
        .expect_err("the bounded session refuses without publishing partial rows");
    assert!(matches!(
        refusal,
        Error::ReadFailure(failure)
            if failure.kind()
                == contextdb_core::read_contract::ReadFailureKind::OwnerLimitExceeded
                && matches!(
                    failure.detail(),
                    ReadFailureDetail::OwnerLimitExceeded(detail)
                        if detail.limit == ReadFailureLimit::ResultRows
                            && detail.value == limits.result_rows
                )
    ));
}

#[test]
fn public_cancellation_and_cursor_shapes_are_lifetime_free() {
    fn assert_static_send_sync<T: 'static + Send + Sync>() {}

    assert_static_send_sync::<ReadSession>();
    assert_static_send_sync::<ReadCursor>();
    let runtime = tempfile::TempDir::new().expect("task-scoped route-shape runtime");
    assert_eq!(
        ReadSession::with_runtime_directory_for_test(runtime.path(), || "scoped-runtime"),
        "scoped-runtime",
    );
    let _: fn(
        &ReadSession,
        &str,
        &HashMap<String, Value>,
        &OwnerReadCancellation,
    ) -> contextdb_core::Result<QueryResult> = ReadSession::execute_with_cancellation;
    let _: fn(
        &ReadSession,
        &str,
        &HashMap<String, Value>,
        &OwnerReadCancellation,
    ) -> contextdb_core::Result<ReadCursor> = ReadSession::open_cursor_with_cancellation;
    let _: fn(
        &mut ReadCursor,
        Option<NonZeroUsize>,
        &OwnerReadCancellation,
    ) -> contextdb_core::Result<CursorPage> = ReadCursor::fetch_with_cancellation;
    let _: fn(&ReadCursor) -> &CursorPage = ReadCursor::first_page;
    let _: fn(
        std::path::PathBuf,
        ReadSessionOptions,
        Arc<dyn ReadKernelTestObserver>,
    ) -> contextdb_core::Result<ReadSession> = ReadSession::open_with_kernel_observer_for_test;
    let _: fn(
        &Database,
        ReadLimits,
        Arc<dyn ReadKernelTestObserver>,
    ) -> contextdb_core::Result<ReadSession> = Database::read_session_with_kernel_observer_for_test;
    let _: Error = Error::ReadCancelled;
}

#[derive(Default)]
struct CancellationObserver {
    tokens: Mutex<Vec<(ReadSessionOperation, OwnerReadCancellation)>>,
}

impl ReadKernelTestObserver for CancellationObserver {
    fn before_source_touch(
        &self,
        event: ReadKernelSourceEvent,
        cancellation: OwnerReadCancellation,
    ) {
        if event.source != ReadKernelSource::TableRow {
            return;
        }
        let mut tokens = self.tokens.lock().expect("cancellation observation lock");
        if !tokens
            .iter()
            .any(|(operation, _)| *operation == event.operation)
        {
            tokens.push((event.operation, cancellation));
        }
    }

    fn cancellation_observed(
        &self,
        _event: ReadKernelCancellationEvent,
        _cancellation: OwnerReadCancellation,
    ) {
    }
}

#[test]
fn no_token_convenience_methods_create_independent_fresh_tokens() {
    let database = Database::open_memory();
    seed_rows(&database, "fresh_token_rows", 3);
    let observer = Arc::new(CancellationObserver::default());
    let limits = ReadLimits {
        cursor_page_rows: 1,
        ..ReadLimits::default()
    };
    let session = database
        .read_session_with_kernel_observer_for_test(limits, observer.clone())
        .expect("attach bounded-kernel observation to the in-process session");

    session
        .execute("SELECT id FROM fresh_token_rows", &HashMap::new())
        .expect("no-token execute reaches the bounded source loop");
    let mut cursor = session
        .open_cursor("SELECT id FROM fresh_token_rows", &HashMap::new())
        .expect("no-token cursor open reaches the bounded source loop");
    assert!(cursor.first_page().has_more);
    cursor
        .fetch(NonZeroUsize::new(1))
        .expect("no-token cursor fetch reaches the bounded source loop");

    let observations = observer
        .tokens
        .lock()
        .expect("cancellation observation lock")
        .clone();
    assert_eq!(
        observations
            .iter()
            .map(|(operation, _)| *operation)
            .collect::<Vec<_>>(),
        vec![
            ReadSessionOperation::Execute,
            ReadSessionOperation::CursorOpen,
            ReadSessionOperation::CursorFetch,
        ]
    );
    observations[0].1.cancel();
    assert!(observations[0].1.is_cancelled());
    assert!(!observations[1].1.is_cancelled());
    assert!(!observations[2].1.is_cancelled());
    observations[1].1.cancel();
    assert!(observations[1].1.is_cancelled());
    assert!(!observations[2].1.is_cancelled());
    observations[2].1.cancel();
    assert!(observations[2].1.is_cancelled());
}

#[test]
fn production_cursor_retains_a_real_multi_page_continuation_after_opening_handles_drop() {
    let database = Database::open_memory();
    let page_rows = [ReadRoute::File, ReadRoute::Owner].len();
    let row_count = page_rows
        .saturating_mul(
            [
                ReadSessionOperation::Execute,
                ReadSessionOperation::CursorOpen,
                ReadSessionOperation::CursorFetch,
            ]
            .len(),
        )
        .saturating_add(1);
    seed_rows(&database, "owned_cursor_rows", row_count);
    let limits = ReadLimits {
        cursor_page_rows: u64::try_from(page_rows).expect("page row count fits policy"),
        ..ReadLimits::default()
    };
    let session = database
        .read_session(limits)
        .expect("create an in-process route owner");
    let mut cursor = session
        .open_cursor(
            "SELECT id FROM owned_cursor_rows ORDER BY id",
            &HashMap::new(),
        )
        .expect("open a production-owned suspended cursor");
    assert_eq!(cursor.retained_state_owners_for_test(), 1);
    assert_eq!(cursor.retained_session_owners_for_test(), 2);
    assert!(cursor.first_page().has_more);
    assert_eq!(
        cursor_page_ids(cursor.first_page()),
        (0..page_rows)
            .map(|id| i64::try_from(id).unwrap())
            .collect::<Vec<_>>(),
    );
    drop(session);
    drop(database);

    assert_eq!(cursor.route(), ReadRoute::Owner);
    assert_eq!(cursor.retained_session_owners_for_test(), 1);
    let next_page = cursor
        .fetch(NonZeroUsize::new(page_rows))
        .expect("the owned continuation produces its next real page");
    assert_eq!(
        cursor_page_ids(&next_page),
        (page_rows..page_rows.saturating_mul(2))
            .map(|id| i64::try_from(id).unwrap())
            .collect::<Vec<_>>(),
    );
    assert!(
        next_page.has_more,
        "the dynamic fixture retains another page"
    );
    cursor.close().expect("close the production-owned cursor");
}
