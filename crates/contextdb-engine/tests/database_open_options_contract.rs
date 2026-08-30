use contextdb_core::read_contract::{
    OwnerReadCancellation, OwnerReadLimits, OwnerRequestHandler, OwnerServiceTimeouts,
};
use contextdb_core::{ContextId, Error, Principal, ScopeLabel};
use contextdb_engine::database::{
    DatabaseOpenInterceptor, open_memory_with_startup_limit, open_with_startup_limits,
};
use contextdb_engine::plugin::QueryOutcome;
use contextdb_engine::read_session::{ReadSessionEvent, ReadSessionTestObserver};
use contextdb_engine::{
    Database, DatabaseOpenOptions, DatabasePlugin, OwnerReadConfig, ReadLimits,
};
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

fn secure_runtime_root(directory: &tempfile::TempDir, name: &str) -> PathBuf {
    let root = directory.path().join(name);
    std::fs::create_dir(&root).expect("create task-scoped owner runtime root");
    #[cfg(unix)]
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("secure task-scoped owner runtime root");
    root
}

#[derive(Default)]
struct PluginCounts {
    opened: AtomicUsize,
    queries: AtomicUsize,
    completed_queries: AtomicUsize,
    closed: AtomicUsize,
}

#[derive(Clone, Default)]
struct CountingPlugin(Arc<PluginCounts>);

impl DatabasePlugin for CountingPlugin {
    fn on_open(&self) -> contextdb_core::Result<()> {
        self.0.opened.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn on_query(&self, _sql: &str) -> contextdb_core::Result<()> {
        self.0.queries.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn post_query(&self, _sql: &str, _duration: Duration, _outcome: &QueryOutcome) {
        self.0.completed_queries.fetch_add(1, Ordering::SeqCst);
    }

    fn on_close(&self) -> contextdb_core::Result<()> {
        self.0.closed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct IdentityHandler;

impl OwnerRequestHandler for IdentityHandler {
    fn handle(
        &self,
        _namespace: &str,
        request: &[u8],
        _cancellation: &OwnerReadCancellation,
    ) -> contextdb_core::Result<Vec<u8>> {
        Ok(request.to_vec())
    }
}

#[derive(Default)]
struct StartupObserver(Mutex<Vec<ReadSessionEvent>>);

impl ReadSessionTestObserver for StartupObserver {
    fn observe_event(&self, event: ReadSessionEvent) {
        self.0.lock().expect("startup observation lock").push(event);
    }
}

impl StartupObserver {
    fn events(&self) -> Vec<ReadSessionEvent> {
        self.0.lock().expect("startup observation lock").clone()
    }

    fn count(&self, expected: ReadSessionEvent) -> usize {
        self.0
            .lock()
            .expect("startup observation lock")
            .iter()
            .filter(|event| **event == expected)
            .count()
    }
}

fn configured_context() -> ContextId {
    ContextId::new(uuid::Uuid::from_u128(
        u128::from(ReadLimits::SHIPPED_RESULT_BYTES)
            .saturating_add(u128::try_from(std::mem::size_of::<ContextId>()).unwrap_or(u128::MAX)),
    ))
}

fn configured_scope() -> ScopeLabel {
    ScopeLabel::new(format!("review-{}", ReadLimits::SHIPPED_CURSOR_PAGE_BYTES))
}

fn configured_principal() -> Principal {
    Principal::Human(format!("operator-{}", ReadLimits::SHIPPED_MEMORY))
}

/// The constructor shapes the surface assertions pin, named so each one is
/// stated once rather than spelled out at every use.
type OpenWithConstraints = fn(
    PathBuf,
    Option<BTreeSet<ContextId>>,
    Option<BTreeSet<ScopeLabel>>,
    Option<Principal>,
) -> contextdb_core::Result<Database>;
type OpenMemoryWithConstraints =
    fn(Option<BTreeSet<ContextId>>, Option<BTreeSet<ScopeLabel>>, Option<Principal>) -> Database;
type OpenWithStartupLimits = fn(
    PathBuf,
    Arc<dyn DatabasePlugin>,
    Option<usize>,
    Option<u64>,
) -> contextdb_core::Result<Database>;

#[test]
fn public_option_and_existing_opener_signatures_are_stable() {
    let _: fn(PathBuf) -> contextdb_core::Result<Database> = Database::open;
    let _: fn() -> Database = Database::open_memory;
    let _: fn(PathBuf, DatabaseOpenOptions) -> contextdb_core::Result<Database> =
        Database::open_with_options;
    let _: fn(PathBuf, BTreeSet<ContextId>) -> contextdb_core::Result<Database> =
        Database::open_with_contexts;
    let _: fn(BTreeSet<ContextId>) -> Database = Database::open_memory_with_contexts;
    let _: fn(PathBuf, BTreeSet<ScopeLabel>) -> contextdb_core::Result<Database> =
        Database::open_with_scope_labels;
    let _: fn(BTreeSet<ScopeLabel>) -> Database = Database::open_memory_with_scope_labels;
    let _: fn(PathBuf, Principal) -> contextdb_core::Result<Database> = Database::open_as_principal;
    let _: fn(Principal) -> Database = Database::open_memory_as_principal;
    let _: OpenWithConstraints = Database::open_with_constraints;
    let _: OpenMemoryWithConstraints = Database::open_memory_with_constraints;
    let _: fn(Arc<dyn DatabasePlugin>) -> contextdb_core::Result<Database> =
        Database::open_memory_with_plugin;
    let _: fn(PathBuf, Arc<dyn DatabasePlugin>) -> contextdb_core::Result<Database> =
        Database::open_with_plugin;
    let _: fn(Arc<dyn DatabasePlugin>, Option<usize>) -> contextdb_core::Result<Database> =
        open_memory_with_startup_limit;
    let _: OpenWithStartupLimits = open_with_startup_limits;
    let database = Database::open_memory();
    let _ = database.owner_read_config();
    let _: Option<usize> = database.memory_limit();
    let _: Option<u64> = database.disk_limit();
    let _: Option<&BTreeSet<ContextId>> = database.contexts();
    let _: Option<&BTreeSet<ScopeLabel>> = database.scope_labels();
    let _: Option<&Principal> = database.principal();
}

#[test]
fn options_defaults_are_complete_and_publicly_inspectable() {
    let options = DatabaseOpenOptions::default();
    assert!(options.owner_reads.enabled);
    assert_eq!(options.owner_reads.limits, OwnerReadLimits::default());
    assert_eq!(
        options.owner_reads.timeouts,
        OwnerServiceTimeouts::default()
    );
    assert!(options.owner_reads.runtime_dir.is_none());
    assert!(options.owner_reads.handler.is_none());
    assert!(options.owner_reads.test_hooks.is_none());
    assert!(options.memory_limit.is_none());
    assert!(options.disk_limit.is_none());
    assert!(options.contexts.is_none());
    assert!(options.scope_labels.is_none());
    assert!(options.principal.is_none());
    assert!(options.test_observer.is_none());
    assert!(options.test_kernel_observer.is_none());
}

#[test]
fn open_options_preserve_owner_limits_plugin_identity_and_access_getters() {
    let directory = tempfile::TempDir::new().expect("task-scoped owner runtime directory");
    let plugin = CountingPlugin::default();
    let startup = Arc::new(StartupObserver::default());
    let configured_plugin: Arc<dyn DatabasePlugin> = Arc::new(plugin.clone());
    let configured_plugin_identity = Arc::as_ptr(&configured_plugin) as *const ();
    let context = configured_context();
    let scope = configured_scope();
    let principal = configured_principal();
    let memory_limit = usize::try_from(ReadLimits::SHIPPED_MEMORY)
        .expect("shipped memory limit fits this platform");
    let owner_limits = OwnerReadLimits {
        limits: ReadLimits {
            result_rows: ReadLimits::default().result_rows.saturating_add(1),
            ..ReadLimits::default()
        },
        concurrency: u64::try_from(std::mem::size_of::<OwnerReadLimits>())
            .expect("owner-limit vocabulary size fits concurrency"),
    };
    let owner_timeouts = OwnerServiceTimeouts {
        request_ms: OwnerServiceTimeouts::default().request_ms.saturating_add(1),
        shutdown_drain_ms: OwnerServiceTimeouts::default()
            .shutdown_drain_ms
            .saturating_add(1),
    };
    let handler: Arc<dyn OwnerRequestHandler> = Arc::new(IdentityHandler);
    let handler_identity = Arc::as_ptr(&handler) as *const ();
    let options = DatabaseOpenOptions {
        plugin: configured_plugin,
        owner_reads: OwnerReadConfig {
            enabled: true,
            limits: owner_limits,
            timeouts: owner_timeouts,
            runtime_dir: Some(directory.path().to_path_buf()),
            handler: Some(handler),
            ..OwnerReadConfig::default()
        },
        memory_limit: Some(memory_limit),
        contexts: Some(BTreeSet::from([context.clone()])),
        scope_labels: Some(BTreeSet::from([scope.clone()])),
        principal: Some(principal.clone()),
        test_observer: Some(startup.clone()),
        ..DatabaseOpenOptions::default()
    };

    let database = Database::open_with_options(":memory:", options)
        .expect("the common options path opens with its complete configuration");
    let startup_events = startup.events();
    assert_eq!(
        startup_events
            .iter()
            .filter(|event| **event == ReadSessionEvent::PluginOpened)
            .count(),
        1
    );
    assert_eq!(
        startup_events
            .iter()
            .filter(|event| **event == ReadSessionEvent::BlobRepositoryOpened)
            .count(),
        1
    );
    assert!(!startup_events.contains(&ReadSessionEvent::OpenRegistryAcquired));
    assert!(!startup_events.contains(&ReadSessionEvent::PersistenceOpened));
    assert!(!startup_events.contains(&ReadSessionEvent::OwnerServiceStarted));
    assert_eq!(
        database.plugin() as *const dyn DatabasePlugin as *const (),
        configured_plugin_identity
    );
    assert!(database.owner_read_config().enabled);
    assert_eq!(database.owner_read_config().limits, owner_limits);
    assert_eq!(database.owner_read_config().timeouts, owner_timeouts);
    assert_eq!(
        database.owner_read_config().runtime_dir.as_deref(),
        Some(directory.path())
    );
    assert_eq!(
        database
            .owner_read_config()
            .handler
            .as_ref()
            .map(|configured| Arc::as_ptr(configured) as *const ()),
        Some(handler_identity)
    );
    assert_eq!(database.memory_limit(), Some(memory_limit));
    assert_eq!(database.disk_limit(), None);
    assert_eq!(database.contexts(), Some(&BTreeSet::from([context])));
    assert_eq!(database.scope_labels(), Some(&BTreeSet::from([scope])));
    assert_eq!(database.principal(), Some(&principal));
    assert_eq!(plugin.0.opened.load(Ordering::SeqCst), 1);
    assert_eq!(plugin.0.queries.load(Ordering::SeqCst), 0);
    assert_eq!(plugin.0.closed.load(Ordering::SeqCst), 0);

    let session = database
        .read_session(ReadLimits::default())
        .expect("session creation shares the configured database");
    assert_eq!(plugin.0.opened.load(Ordering::SeqCst), 1);
    session
        .execute("SELECT 1", &HashMap::new())
        .expect("bounded query reaches the configured plugin exactly once");
    assert_eq!(plugin.0.queries.load(Ordering::SeqCst), 1);
    assert_eq!(plugin.0.completed_queries.load(Ordering::SeqCst), 1);
    drop(session);
    assert_eq!(plugin.0.closed.load(Ordering::SeqCst), 0);
    assert_eq!(startup.events(), startup_events);
    database.close().expect("configured database closes");
    assert_eq!(plugin.0.closed.load(Ordering::SeqCst), 1);
}

#[test]
fn memory_open_rejects_a_disk_limit_before_any_startup_resource_is_created() {
    let plugin = CountingPlugin::default();
    let startup = Arc::new(StartupObserver::default());
    let options = DatabaseOpenOptions {
        plugin: Arc::new(plugin.clone()),
        disk_limit: Some(ReadLimits::SHIPPED_RESULT_BYTES),
        test_observer: Some(startup.clone()),
        ..DatabaseOpenOptions::default()
    };

    assert!(matches!(
        Database::open_with_options(":memory:", options),
        Err(Error::Other(_))
    ));
    assert_eq!(plugin.0.opened.load(Ordering::SeqCst), 0);
    assert_eq!(plugin.0.queries.load(Ordering::SeqCst), 0);
    assert_eq!(plugin.0.closed.load(Ordering::SeqCst), 0);
    assert!(
        startup
            .0
            .lock()
            .expect("startup observation lock")
            .is_empty()
    );
}

#[test]
fn file_open_options_expose_the_configured_disk_ceiling() {
    let directory = tempfile::TempDir::new().expect("task-scoped disk-option directory");
    let runtime_root = secure_runtime_root(&directory, "disk-option-runtime");
    let disk_limit = ReadLimits::SHIPPED_RESULT_BYTES.saturating_mul(4);
    let startup = Arc::new(StartupObserver::default());
    // The nested runtime directory is set after the literal so the rest of
    // the owner-read configuration stays exactly what the default carries.
    let mut options = DatabaseOpenOptions {
        disk_limit: Some(disk_limit),
        test_observer: Some(startup.clone()),
        ..DatabaseOpenOptions::default()
    };
    options.owner_reads.runtime_dir = Some(runtime_root);
    let database = Database::open_with_options(directory.path().join("disk-option.db"), options)
        .expect("file-backed common options accept a disk ceiling");
    assert_eq!(database.disk_limit(), Some(disk_limit));
    assert_eq!(startup.count(ReadSessionEvent::OpenRegistryAcquired), 1);
    assert_eq!(startup.count(ReadSessionEvent::PersistenceOpened), 1);
    assert_eq!(startup.count(ReadSessionEvent::BlobRepositoryOpened), 1);
    assert_eq!(startup.count(ReadSessionEvent::PluginOpened), 1);
    assert_eq!(startup.count(ReadSessionEvent::OwnerServiceStarted), 1);
    database.close().expect("close disk-option database");
}

#[test]
fn scoped_handles_share_the_open_database_without_restarting_its_plugin() {
    let plugin = CountingPlugin::default();
    let database = Database::open_memory_with_plugin(Arc::new(plugin.clone()))
        .expect("open a database with one plugin lifecycle");
    let interceptor = Arc::new(FullOptionsInterceptor::new());
    let scoped = Database::with_common_open_interceptor_for_test(interceptor.clone(), || {
        database.scoped_with_constraints(
            Some(BTreeSet::from([configured_context()])),
            Some(BTreeSet::from([configured_scope()])),
            Some(configured_principal()),
        )
    });
    assert!(
        interceptor
            .calls
            .lock()
            .expect("common-open interception lock")
            .is_empty(),
        "a scoped handle is a narrowed shared handle, never an open",
    );
    assert_eq!(plugin.0.opened.load(Ordering::SeqCst), 1);
    drop(scoped);
    assert_eq!(plugin.0.opened.load(Ordering::SeqCst), 1);
    assert_eq!(plugin.0.closed.load(Ordering::SeqCst), 0);
    database
        .close()
        .expect("resource owner closes the shared database once");
    assert_eq!(plugin.0.closed.load(Ordering::SeqCst), 1);
}

#[derive(Debug, Clone, PartialEq)]
struct InterceptedOpen {
    path: PathBuf,
    owner_enabled: bool,
    owner_limits: OwnerReadLimits,
    owner_timeouts: OwnerServiceTimeouts,
    owner_runtime_dir: Option<PathBuf>,
    owner_handler: bool,
    owner_test_hooks: bool,
    plugin_identity: usize,
    memory_limit: Option<usize>,
    disk_limit: Option<u64>,
    contexts: Option<BTreeSet<ContextId>>,
    scope_labels: Option<BTreeSet<ScopeLabel>>,
    principal: Option<Principal>,
    route_observer: bool,
    kernel_observer: bool,
}

impl InterceptedOpen {
    fn capture(path: &Path, options: &DatabaseOpenOptions) -> Self {
        Self {
            path: path.to_path_buf(),
            owner_enabled: options.owner_reads.enabled,
            owner_limits: options.owner_reads.limits,
            owner_timeouts: options.owner_reads.timeouts,
            owner_runtime_dir: options.owner_reads.runtime_dir.clone(),
            owner_handler: options.owner_reads.handler.is_some(),
            owner_test_hooks: options.owner_reads.test_hooks.is_some(),
            plugin_identity: Arc::as_ptr(&options.plugin) as *const () as usize,
            memory_limit: options.memory_limit,
            disk_limit: options.disk_limit,
            contexts: options.contexts.clone(),
            scope_labels: options.scope_labels.clone(),
            principal: options.principal.clone(),
            route_observer: options.test_observer.is_some(),
            kernel_observer: options.test_kernel_observer.is_some(),
        }
    }
}

struct FullOptionsInterceptor {
    refusal: String,
    calls: Mutex<Vec<InterceptedOpen>>,
}

impl FullOptionsInterceptor {
    fn new() -> Self {
        Self {
            refusal: format!("intercepted-common-open:{}", uuid::Uuid::new_v4()),
            calls: Mutex::new(Vec::new()),
        }
    }

    fn only_call(&self) -> InterceptedOpen {
        let calls = self.calls.lock().expect("common-open interception lock");
        assert_eq!(
            calls.len(),
            1,
            "every wrapper must reach the common opener exactly once",
        );
        calls[0].clone()
    }
}

impl DatabaseOpenInterceptor for FullOptionsInterceptor {
    fn intercept(
        &self,
        path: &Path,
        options: &DatabaseOpenOptions,
    ) -> contextdb_core::Result<Database> {
        self.calls
            .lock()
            .expect("common-open interception lock")
            .push(InterceptedOpen::capture(path, options));
        Err(Error::Other(self.refusal.clone()))
    }
}

fn intercept_result_opener(
    open: impl FnOnce() -> contextdb_core::Result<Database>,
) -> InterceptedOpen {
    let interceptor = Arc::new(FullOptionsInterceptor::new());
    let result = Database::with_common_open_interceptor_for_test(interceptor.clone(), open);
    match result {
        Err(Error::Other(message)) if message == interceptor.refusal => {}
        Ok(database) => {
            database
                .close()
                .expect("close opener that bypassed interception");
            panic!("ordinary opener bypassed the common options entrance");
        }
        Err(error) => panic!("ordinary opener returned before common interception: {error}"),
    }
    let call = interceptor.only_call();
    assert_ordinary_owner_and_observer_defaults(&call);
    call
}

fn intercept_infallible_opener(open: impl FnOnce() -> Database) -> InterceptedOpen {
    let interceptor = Arc::new(FullOptionsInterceptor::new());
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        Database::with_common_open_interceptor_for_test(interceptor.clone(), open)
    }));
    if let Ok(database) = outcome {
        database
            .close()
            .expect("close opener that bypassed interception");
        panic!("infallible opener bypassed the common options entrance");
    }
    let call = interceptor.only_call();
    assert_ordinary_owner_and_observer_defaults(&call);
    call
}

fn assert_ordinary_owner_and_observer_defaults(call: &InterceptedOpen) {
    assert!(call.owner_enabled);
    assert_eq!(call.owner_limits, OwnerReadLimits::default());
    assert_eq!(call.owner_timeouts, OwnerServiceTimeouts::default());
    assert!(call.owner_runtime_dir.is_none());
    assert!(!call.owner_handler);
    assert!(!call.owner_test_hooks);
    assert_ne!(call.plugin_identity, 0);
    assert!(!call.route_observer);
    assert!(!call.kernel_observer);
}

fn assert_default_open_options(call: &InterceptedOpen) {
    assert_ordinary_owner_and_observer_defaults(call);
    assert!(call.memory_limit.is_none());
    assert!(call.disk_limit.is_none());
    assert!(call.contexts.is_none());
    assert!(call.scope_labels.is_none());
    assert!(call.principal.is_none());
}

#[test]
fn file_opener_passes_complete_defaults_once_to_common_options() {
    let directory = tempfile::TempDir::new().expect("task-scoped file opener directory");
    let path = directory.path().join("plain.db");
    let call = intercept_result_opener(|| Database::open(&path));
    assert_eq!(call.path, path);
    assert_default_open_options(&call);
}

#[test]
fn memory_opener_passes_complete_defaults_once_to_common_options() {
    let call = intercept_infallible_opener(Database::open_memory);
    assert_eq!(call.path, PathBuf::from(":memory:"));
    assert_default_open_options(&call);
}

#[test]
fn file_context_opener_passes_its_constraint_once_to_common_options() {
    let directory = tempfile::TempDir::new().expect("task-scoped context opener directory");
    let path = directory.path().join("contexts.db");
    let contexts = BTreeSet::from([configured_context()]);
    let call = intercept_result_opener(|| Database::open_with_contexts(&path, contexts.clone()));
    assert_eq!(call.path, path);
    assert_eq!(call.contexts, Some(contexts));
    assert!(call.scope_labels.is_none());
    assert!(call.principal.is_none());
    assert!(call.memory_limit.is_none());
    assert!(call.disk_limit.is_none());
}

#[test]
fn memory_context_opener_passes_its_constraint_once_to_common_options() {
    let contexts = BTreeSet::from([configured_context()]);
    let call =
        intercept_infallible_opener(|| Database::open_memory_with_contexts(contexts.clone()));
    assert_eq!(call.path, PathBuf::from(":memory:"));
    assert_eq!(call.contexts, Some(contexts));
    assert!(call.scope_labels.is_none());
    assert!(call.principal.is_none());
    assert!(call.memory_limit.is_none());
    assert!(call.disk_limit.is_none());
}

#[test]
fn file_scope_opener_passes_its_constraint_once_to_common_options() {
    let directory = tempfile::TempDir::new().expect("task-scoped scope opener directory");
    let path = directory.path().join("scopes.db");
    let scopes = BTreeSet::from([configured_scope()]);
    let call = intercept_result_opener(|| Database::open_with_scope_labels(&path, scopes.clone()));
    assert_eq!(call.path, path);
    assert_eq!(call.scope_labels, Some(scopes));
    assert!(call.contexts.is_none());
    assert!(call.principal.is_none());
    assert!(call.memory_limit.is_none());
    assert!(call.disk_limit.is_none());
}

#[test]
fn memory_scope_opener_passes_its_constraint_once_to_common_options() {
    let scopes = BTreeSet::from([configured_scope()]);
    let call =
        intercept_infallible_opener(|| Database::open_memory_with_scope_labels(scopes.clone()));
    assert_eq!(call.path, PathBuf::from(":memory:"));
    assert_eq!(call.scope_labels, Some(scopes));
    assert!(call.contexts.is_none());
    assert!(call.principal.is_none());
    assert!(call.memory_limit.is_none());
    assert!(call.disk_limit.is_none());
}

#[test]
fn file_principal_opener_passes_its_principal_once_to_common_options() {
    let directory = tempfile::TempDir::new().expect("task-scoped principal opener directory");
    let path = directory.path().join("principal.db");
    let principal = configured_principal();
    let call = intercept_result_opener(|| Database::open_as_principal(&path, principal.clone()));
    assert_eq!(call.path, path);
    assert_eq!(call.principal, Some(principal));
    assert!(call.contexts.is_none());
    assert!(call.scope_labels.is_none());
    assert!(call.memory_limit.is_none());
    assert!(call.disk_limit.is_none());
}

#[test]
fn memory_principal_opener_passes_its_principal_once_to_common_options() {
    let principal = configured_principal();
    let call =
        intercept_infallible_opener(|| Database::open_memory_as_principal(principal.clone()));
    assert_eq!(call.path, PathBuf::from(":memory:"));
    assert_eq!(call.principal, Some(principal));
    assert!(call.contexts.is_none());
    assert!(call.scope_labels.is_none());
    assert!(call.memory_limit.is_none());
    assert!(call.disk_limit.is_none());
}

#[test]
fn file_constraint_opener_passes_every_constraint_once_to_common_options() {
    let directory = tempfile::TempDir::new().expect("task-scoped constraint opener directory");
    let path = directory.path().join("constraints.db");
    let contexts = BTreeSet::from([configured_context()]);
    let scopes = BTreeSet::from([configured_scope()]);
    let principal = configured_principal();
    let call = intercept_result_opener(|| {
        Database::open_with_constraints(
            &path,
            Some(contexts.clone()),
            Some(scopes.clone()),
            Some(principal.clone()),
        )
    });
    assert_eq!(call.path, path);
    assert_eq!(call.contexts, Some(contexts));
    assert_eq!(call.scope_labels, Some(scopes));
    assert_eq!(call.principal, Some(principal));
    assert!(call.memory_limit.is_none());
    assert!(call.disk_limit.is_none());
}

#[test]
fn memory_constraint_opener_passes_every_constraint_once_to_common_options() {
    let contexts = BTreeSet::from([configured_context()]);
    let scopes = BTreeSet::from([configured_scope()]);
    let principal = configured_principal();
    let call = intercept_infallible_opener(|| {
        Database::open_memory_with_constraints(
            Some(contexts.clone()),
            Some(scopes.clone()),
            Some(principal.clone()),
        )
    });
    assert_eq!(call.path, PathBuf::from(":memory:"));
    assert_eq!(call.contexts, Some(contexts));
    assert_eq!(call.scope_labels, Some(scopes));
    assert_eq!(call.principal, Some(principal));
    assert!(call.memory_limit.is_none());
    assert!(call.disk_limit.is_none());
}

#[test]
fn file_plugin_opener_passes_the_same_plugin_once_to_common_options() {
    let directory = tempfile::TempDir::new().expect("task-scoped plugin opener directory");
    let path = directory.path().join("plugin.db");
    let plugin: Arc<dyn DatabasePlugin> = Arc::new(CountingPlugin::default());
    let identity = Arc::as_ptr(&plugin) as *const () as usize;
    let call = intercept_result_opener(|| Database::open_with_plugin(&path, plugin.clone()));
    assert_eq!(call.path, path);
    assert_eq!(call.plugin_identity, identity);
    assert!(call.memory_limit.is_none());
    assert!(call.disk_limit.is_none());
    assert!(call.contexts.is_none());
    assert!(call.scope_labels.is_none());
    assert!(call.principal.is_none());
}

#[test]
fn memory_plugin_opener_passes_the_same_plugin_once_to_common_options() {
    let plugin: Arc<dyn DatabasePlugin> = Arc::new(CountingPlugin::default());
    let identity = Arc::as_ptr(&plugin) as *const () as usize;
    let call = intercept_result_opener(|| Database::open_memory_with_plugin(plugin.clone()));
    assert_eq!(call.path, PathBuf::from(":memory:"));
    assert_eq!(call.plugin_identity, identity);
    assert!(call.memory_limit.is_none());
    assert!(call.disk_limit.is_none());
    assert!(call.contexts.is_none());
    assert!(call.scope_labels.is_none());
    assert!(call.principal.is_none());
}

#[test]
fn file_startup_limit_opener_passes_all_limits_and_plugin_once_to_common_options() {
    let directory = tempfile::TempDir::new().expect("task-scoped startup-limit directory");
    let path = directory.path().join("startup-limits.db");
    let memory_limit = usize::try_from(ReadLimits::SHIPPED_MEMORY)
        .expect("shipped memory limit fits this platform");
    let disk_limit = ReadLimits::SHIPPED_RESULT_BYTES;
    let plugin: Arc<dyn DatabasePlugin> = Arc::new(CountingPlugin::default());
    let identity = Arc::as_ptr(&plugin) as *const () as usize;
    let call = intercept_result_opener(|| {
        open_with_startup_limits(&path, plugin.clone(), Some(memory_limit), Some(disk_limit))
    });
    assert_eq!(call.path, path);
    assert_eq!(call.plugin_identity, identity);
    assert_eq!(call.memory_limit, Some(memory_limit));
    assert_eq!(call.disk_limit, Some(disk_limit));
    assert!(call.contexts.is_none());
    assert!(call.scope_labels.is_none());
    assert!(call.principal.is_none());
}

#[test]
fn memory_startup_limit_opener_passes_its_limit_and_plugin_once_to_common_options() {
    let memory_limit = usize::try_from(ReadLimits::SHIPPED_MEMORY)
        .expect("shipped memory limit fits this platform");
    let plugin: Arc<dyn DatabasePlugin> = Arc::new(CountingPlugin::default());
    let identity = Arc::as_ptr(&plugin) as *const () as usize;
    let call = intercept_result_opener(|| {
        open_memory_with_startup_limit(plugin.clone(), Some(memory_limit))
    });
    assert_eq!(call.path, PathBuf::from(":memory:"));
    assert_eq!(call.plugin_identity, identity);
    assert_eq!(call.memory_limit, Some(memory_limit));
    assert!(call.disk_limit.is_none());
    assert!(call.contexts.is_none());
    assert!(call.scope_labels.is_none());
    assert!(call.principal.is_none());
}
