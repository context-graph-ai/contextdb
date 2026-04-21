use contextdb_core::Lsn;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use contextdb_core::{Error, Result, Value};
use contextdb_engine::database::Database;
use contextdb_engine::plugin::*;
use contextdb_engine::sync_types::{
    ChangeSet, ConflictPolicies, ConflictPolicy, DdlChange, NaturalKey, RowChange,
};
use contextdb_tx::WriteSet;
use uuid::Uuid;

// ── RejectingPlugin ──────────────────────────────────────────────
// Rejects at a specific hook with a specific reason.
struct RejectingPlugin {
    hook: String,
    reason: String,
}

impl RejectingPlugin {
    fn new(hook: &str, reason: &str) -> Self {
        Self {
            hook: hook.to_string(),
            reason: reason.to_string(),
        }
    }
    fn reject(&self, at_hook: &str) -> Result<()> {
        if self.hook == at_hook {
            Err(Error::PluginRejected {
                hook: at_hook.to_string(),
                reason: self.reason.clone(),
            })
        } else {
            Ok(())
        }
    }
}

impl DatabasePlugin for RejectingPlugin {
    fn pre_commit(&self, _ws: &WriteSet, _source: CommitSource) -> Result<()> {
        self.reject("pre_commit")
    }
    fn on_open(&self) -> Result<()> {
        self.reject("on_open")
    }
    fn on_close(&self) -> Result<()> {
        self.reject("on_close")
    }
    fn on_ddl(&self, _change: &DdlChange) -> Result<()> {
        self.reject("on_ddl")
    }
    fn on_query(&self, _sql: &str) -> Result<()> {
        self.reject("on_query")
    }
    fn on_sync_push(&self, _cs: &mut ChangeSet) -> Result<()> {
        self.reject("on_sync_push")
    }
    fn on_sync_pull(&self, _cs: &mut ChangeSet) -> Result<()> {
        self.reject("on_sync_pull")
    }
}

// ── CapturingPlugin ──────────────────────────────────────────────
// Records every hook call with its arguments.
#[derive(Debug, Clone, PartialEq)]
enum HookEvent {
    PreCommit(CommitSource, Option<Lsn>),  // source, commit_lsn
    PostCommit(CommitSource, Option<Lsn>), // source, commit_lsn
    OnOpen,
    OnClose,
    OnQuery(String),
    OnDdl(String), // ddl description
    PostQuery(String, QueryOutcome),
    OnSyncPush,
    OnSyncPull,
}

struct CapturingPlugin {
    events: Arc<Mutex<Vec<HookEvent>>>,
}

impl CapturingPlugin {
    fn new() -> (Self, Arc<Mutex<Vec<HookEvent>>>) {
        let events = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                events: events.clone(),
            },
            events,
        )
    }
}

impl DatabasePlugin for CapturingPlugin {
    fn pre_commit(&self, ws: &WriteSet, source: CommitSource) -> Result<()> {
        self.events
            .lock()
            .unwrap()
            .push(HookEvent::PreCommit(source, ws.commit_lsn));
        Ok(())
    }
    fn post_commit(&self, ws: &WriteSet, source: CommitSource) {
        self.events
            .lock()
            .unwrap()
            .push(HookEvent::PostCommit(source, ws.commit_lsn));
    }
    fn on_open(&self) -> Result<()> {
        self.events.lock().unwrap().push(HookEvent::OnOpen);
        Ok(())
    }
    fn on_close(&self) -> Result<()> {
        self.events.lock().unwrap().push(HookEvent::OnClose);
        Ok(())
    }
    fn on_query(&self, sql: &str) -> Result<()> {
        self.events
            .lock()
            .unwrap()
            .push(HookEvent::OnQuery(sql.to_string()));
        Ok(())
    }
    fn on_ddl(&self, change: &DdlChange) -> Result<()> {
        self.events
            .lock()
            .unwrap()
            .push(HookEvent::OnDdl(format!("{change:?}")));
        Ok(())
    }
    fn post_query(&self, sql: &str, _duration: Duration, outcome: &QueryOutcome) {
        self.events
            .lock()
            .unwrap()
            .push(HookEvent::PostQuery(sql.to_string(), outcome.clone()));
    }
    fn on_sync_push(&self, _cs: &mut ChangeSet) -> Result<()> {
        self.events.lock().unwrap().push(HookEvent::OnSyncPush);
        Ok(())
    }
    fn on_sync_pull(&self, _cs: &mut ChangeSet) -> Result<()> {
        self.events.lock().unwrap().push(HookEvent::OnSyncPull);
        Ok(())
    }
}

// ── FilterPlugin ─────────────────────────────────────────────────
// Removes rows matching a table name from sync changesets.
struct FilterPlugin {
    blocked_table: String,
}

impl FilterPlugin {
    fn new(table: &str) -> Self {
        Self {
            blocked_table: table.to_string(),
        }
    }
}

impl DatabasePlugin for FilterPlugin {
    fn on_sync_push(&self, cs: &mut ChangeSet) -> Result<()> {
        cs.rows.retain(|r| r.table != self.blocked_table);
        Ok(())
    }
    fn on_sync_pull(&self, cs: &mut ChangeSet) -> Result<()> {
        cs.rows.retain(|r| r.table != self.blocked_table);
        Ok(())
    }
}

// ── OrderingPlugin ───────────────────────────────────────────────
// Records a label into a shared vec to prove firing order.
struct OrderingPlugin {
    label: String,
    log: Arc<Mutex<Vec<String>>>,
}

impl OrderingPlugin {
    fn new(label: &str, log: Arc<Mutex<Vec<String>>>) -> Self {
        Self {
            label: label.to_string(),
            log,
        }
    }
}

impl DatabasePlugin for OrderingPlugin {
    fn pre_commit(&self, _ws: &WriteSet, _source: CommitSource) -> Result<()> {
        self.log
            .lock()
            .unwrap()
            .push(format!("{}:pre_commit", self.label));
        Ok(())
    }
    fn on_query(&self, _sql: &str) -> Result<()> {
        self.log
            .lock()
            .unwrap()
            .push(format!("{}:on_query", self.label));
        Ok(())
    }
}

// ── SelectiveRejectPlugin ────────────────────────────────────────
// Rejects on_query for SQL containing a specific substring,
// while capturing post_query to verify it does NOT fire.
struct SelectiveRejectPlugin {
    reject_sql_containing: String,
    post_query_calls: Arc<Mutex<Vec<String>>>,
}

impl SelectiveRejectPlugin {
    fn new(reject_pattern: &str) -> (Self, Arc<Mutex<Vec<String>>>) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                reject_sql_containing: reject_pattern.to_string(),
                post_query_calls: calls.clone(),
            },
            calls,
        )
    }
}

impl DatabasePlugin for SelectiveRejectPlugin {
    fn on_query(&self, sql: &str) -> Result<()> {
        if sql
            .to_uppercase()
            .contains(&self.reject_sql_containing.to_uppercase())
        {
            Err(Error::PluginRejected {
                hook: "on_query".to_string(),
                reason: format!("blocked: {}", self.reject_sql_containing),
            })
        } else {
            Ok(())
        }
    }
    fn post_query(&self, sql: &str, _d: Duration, _o: &QueryOutcome) {
        self.post_query_calls.lock().unwrap().push(sql.to_string());
    }
}

// ── RejectAndCapturePlugin (for P08) ─────────────────────────────
struct RejectAndCapturePlugin {
    reject_hook: String,
    reason: String,
    post_events: Arc<Mutex<Vec<HookEvent>>>,
}

impl DatabasePlugin for RejectAndCapturePlugin {
    fn pre_commit(&self, _ws: &WriteSet, _source: CommitSource) -> Result<()> {
        if self.reject_hook == "pre_commit" {
            Err(Error::PluginRejected {
                hook: "pre_commit".to_string(),
                reason: self.reason.clone(),
            })
        } else {
            Ok(())
        }
    }
    fn post_commit(&self, ws: &WriteSet, source: CommitSource) {
        self.post_events
            .lock()
            .unwrap()
            .push(HookEvent::PostCommit(source, ws.commit_lsn));
    }
}

// ── DdlRejectWithPostCapture (for P22) ───────────────────────────
struct DdlRejectWithPostCapture {
    reason: String,
    post_query_calls: Arc<Mutex<Vec<String>>>,
}

impl DatabasePlugin for DdlRejectWithPostCapture {
    fn on_ddl(&self, _change: &DdlChange) -> Result<()> {
        Err(Error::PluginRejected {
            hook: "on_ddl".to_string(),
            reason: self.reason.clone(),
        })
    }
    fn post_query(&self, sql: &str, _d: Duration, _o: &QueryOutcome) {
        self.post_query_calls.lock().unwrap().push(sql.to_string());
    }
}

// ── CustomHealthPlugin (for P26) ─────────────────────────────────
struct CustomHealthPlugin;
impl DatabasePlugin for CustomHealthPlugin {
    fn health(&self) -> PluginHealth {
        PluginHealth::Degraded("disk-pressure".to_string())
    }
}

// ── CustomDescribePlugin (for P27) ───────────────────────────────
struct CustomDescribePlugin;
impl DatabasePlugin for CustomDescribePlugin {
    fn describe(&self) -> serde_json::Value {
        serde_json::json!({"name": "audit-plugin", "version": "1.0"})
    }
}

// ── DecoratorPlugin (for P28, P29) ───────────────────────────────
struct DecoratorPlugin {
    outer: Box<dyn DatabasePlugin>,
    inner: Box<dyn DatabasePlugin>,
}

impl DecoratorPlugin {
    fn new(outer: Box<dyn DatabasePlugin>, inner: Box<dyn DatabasePlugin>) -> Self {
        Self { outer, inner }
    }
}

impl DatabasePlugin for DecoratorPlugin {
    fn pre_commit(&self, ws: &WriteSet, source: CommitSource) -> Result<()> {
        self.inner.pre_commit(ws, source)?;
        self.outer.pre_commit(ws, source)
    }
    fn post_commit(&self, ws: &WriteSet, source: CommitSource) {
        self.inner.post_commit(ws, source);
        self.outer.post_commit(ws, source);
    }
    fn on_query(&self, sql: &str) -> Result<()> {
        self.inner.on_query(sql)?;
        self.outer.on_query(sql)
    }
    fn on_ddl(&self, change: &DdlChange) -> Result<()> {
        self.inner.on_ddl(change)?;
        self.outer.on_ddl(change)
    }
    fn post_query(&self, sql: &str, d: Duration, o: &QueryOutcome) {
        self.inner.post_query(sql, d, o);
        self.outer.post_query(sql, d, o);
    }
    fn on_open(&self) -> Result<()> {
        self.inner.on_open()?;
        self.outer.on_open()
    }
    fn on_close(&self) -> Result<()> {
        self.inner.on_close()?;
        self.outer.on_close()
    }
    fn on_sync_push(&self, cs: &mut ChangeSet) -> Result<()> {
        self.inner.on_sync_push(cs)?;
        self.outer.on_sync_push(cs)
    }
    fn on_sync_pull(&self, cs: &mut ChangeSet) -> Result<()> {
        self.inner.on_sync_pull(cs)?;
        self.outer.on_sync_pull(cs)
    }
    fn health(&self) -> PluginHealth {
        self.outer.health()
    }
    fn describe(&self) -> serde_json::Value {
        self.outer.describe()
    }
}

// ── DdlCapturingPlugin ───────────────────────────────────────────
// Captures actual DdlChange values for precise variant assertions.
struct DdlCapturingPlugin {
    changes: Arc<Mutex<Vec<DdlChange>>>,
}

impl DatabasePlugin for DdlCapturingPlugin {
    fn on_ddl(&self, change: &DdlChange) -> Result<()> {
        self.changes.lock().unwrap().push(change.clone());
        Ok(())
    }
}

// ── DdlVetoPlugin ────────────────────────────────────────────────
// Rejects on_ddl for a specific DdlChange variant (by name).
struct DdlVetoPlugin {
    reject_variant: String,
}

impl DatabasePlugin for DdlVetoPlugin {
    fn on_ddl(&self, change: &DdlChange) -> Result<()> {
        let variant_name = match change {
            DdlChange::CreateTable { .. } => "CreateTable",
            DdlChange::DropTable { .. } => "DropTable",
            DdlChange::AlterTable { .. } => "AlterTable",
        };
        if variant_name == self.reject_variant {
            Err(Error::PluginRejected {
                hook: "on_ddl".to_string(),
                reason: format!("{} is blocked by policy", variant_name),
            })
        } else {
            Ok(())
        }
    }
}

// ══════════════════════════════════════════════════════════════════
// Test Functions
// ══════════════════════════════════════════════════════════════════

#[test]
fn p01_pre_commit_vetoes_autocommit() {
    let plugin = RejectingPlugin::new("pre_commit", "audit-required");
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    let p = HashMap::new();
    db.execute("CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)", &p)
        .unwrap();

    let id = Uuid::new_v4();
    let result = db.execute(
        "INSERT INTO items (id, name) VALUES ($id, 'widget')",
        &HashMap::from([("id".to_string(), Value::Uuid(id))]),
    );

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(err, Error::PluginRejected { ref hook, .. } if hook == "pre_commit"));
    // Display impl must include hook name and reason (covers P30's intent)
    let msg = format!("{err}");
    assert!(
        msg.contains("pre_commit"),
        "error Display must contain hook name"
    );
    assert!(
        msg.contains("audit-required"),
        "error Display must contain reason"
    );
    assert!(db.scan("items", db.snapshot()).unwrap().is_empty());
}

#[test]
fn p02_pre_commit_vetoes_explicit_commit() {
    let plugin = RejectingPlugin::new("pre_commit", "forbidden");
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    let p = HashMap::new();
    db.execute("CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)", &p)
        .unwrap();

    db.execute("BEGIN", &p).unwrap();
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO items (id, name) VALUES ($id, 'widget')",
        &HashMap::from([("id".to_string(), Value::Uuid(id))]),
    )
    .unwrap();

    let result = db.execute("COMMIT", &p);
    assert!(result.is_err());
    assert!(
        matches!(result.unwrap_err(), Error::PluginRejected { ref hook, .. } if hook == "pre_commit")
    );
    // Verify the row was NOT applied
    assert!(
        db.scan("items", db.snapshot()).unwrap().is_empty(),
        "data must not persist after pre_commit veto"
    );
}

#[test]
fn p03_pre_commit_source_autocommit() {
    let (plugin, events) = CapturingPlugin::new();
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    let p = HashMap::new();
    db.execute("CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)", &p)
        .unwrap();
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO items (id, name) VALUES ($id, 'widget')",
        &HashMap::from([("id".to_string(), Value::Uuid(id))]),
    )
    .unwrap();

    let ev = events.lock().unwrap();
    let pre_commits: Vec<_> = ev
        .iter()
        .filter(|e| matches!(e, HookEvent::PreCommit(..)))
        .collect();
    assert_eq!(
        pre_commits.len(),
        1,
        "exactly one pre_commit for the INSERT (DDL has no WriteSet)"
    );
    assert!(matches!(
        pre_commits[0],
        HookEvent::PreCommit(CommitSource::AutoCommit, _)
    ));
    // pre_commit fires BEFORE stamp_and_apply — commit_lsn must be None at this point
    assert!(
        matches!(pre_commits[0], HookEvent::PreCommit(_, None)),
        "pre_commit must see unstamped WriteSet (commit_lsn == None)"
    );
}

#[test]
fn p04_pre_commit_source_user() {
    let (plugin, events) = CapturingPlugin::new();
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    let p = HashMap::new();
    db.execute("CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)", &p)
        .unwrap();
    db.execute("BEGIN", &p).unwrap();
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO items (id, name) VALUES ($id, 'widget')",
        &HashMap::from([("id".to_string(), Value::Uuid(id))]),
    )
    .unwrap();
    db.execute("COMMIT", &p).unwrap();

    let ev = events.lock().unwrap();
    let user_commits: Vec<_> = ev
        .iter()
        .filter(|e| matches!(e, HookEvent::PreCommit(CommitSource::User, _)))
        .collect();
    assert_eq!(
        user_commits.len(),
        1,
        "exactly one pre_commit with CommitSource::User for the explicit COMMIT"
    );
}

#[test]
fn p05_pre_commit_source_sync_pull() {
    let (plugin, events) = CapturingPlugin::new();
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    let p = HashMap::new();
    db.execute("CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)", &p)
        .unwrap();

    let cs = ChangeSet {
        rows: vec![RowChange {
            table: "items".to_string(),
            natural_key: NaturalKey {
                column: "id".to_string(),
                value: Value::Uuid(Uuid::new_v4()),
            },
            values: HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("name".to_string(), Value::Text("synced".to_string())),
            ]),
            deleted: false,
            lsn: Lsn(1),
        }],
        edges: vec![],
        vectors: vec![],
        ddl: vec![],
    };
    db.apply_changes(cs, &ConflictPolicies::uniform(ConflictPolicy::LatestWins))
        .unwrap();

    let ev = events.lock().unwrap();
    assert!(
        ev.iter()
            .any(|e| matches!(e, HookEvent::PreCommit(CommitSource::SyncPull, _)))
    );
}

#[test]
fn p06_post_commit_fires_with_lsn_autocommit() {
    let (plugin, events) = CapturingPlugin::new();
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    let p = HashMap::new();
    db.execute("CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)", &p)
        .unwrap();
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO items (id, name) VALUES ($id, 'widget')",
        &HashMap::from([("id".to_string(), Value::Uuid(id))]),
    )
    .unwrap();

    let current_lsn = db.current_lsn();
    let ev = events.lock().unwrap();
    let posts: Vec<_> = ev
        .iter()
        .filter(|e| matches!(e, HookEvent::PostCommit(CommitSource::AutoCommit, _)))
        .collect();
    assert_eq!(posts.len(), 1, "exactly one post_commit for the INSERT");
    assert!(
        matches!(posts[0], HookEvent::PostCommit(CommitSource::AutoCommit, Some(lsn)) if lsn.0 > 0)
    );
    // Cross-check: captured LSN matches the database's current LSN
    if let HookEvent::PostCommit(_, Some(captured_lsn)) = posts[0] {
        assert_eq!(
            *captured_lsn, current_lsn,
            "post_commit LSN must match db.current_lsn()"
        );
    }

    // Positive-path watermark check: committed data is visible via snapshot
    drop(ev);
    assert_eq!(
        db.scan("items", db.snapshot()).unwrap().len(),
        1,
        "committed row must be visible — stamp_and_apply must advance watermark"
    );
}

#[test]
fn p07_post_commit_fires_with_lsn_explicit() {
    let (plugin, events) = CapturingPlugin::new();
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    let p = HashMap::new();
    db.execute("CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)", &p)
        .unwrap();
    db.execute("BEGIN", &p).unwrap();
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO items (id, name) VALUES ($id, 'a')",
        &HashMap::from([("id".to_string(), Value::Uuid(id))]),
    )
    .unwrap();
    db.execute("COMMIT", &p).unwrap();

    let ev = events.lock().unwrap();
    let posts: Vec<_> = ev
        .iter()
        .filter(|e| matches!(e, HookEvent::PostCommit(CommitSource::User, _)))
        .collect();
    assert_eq!(posts.len(), 1);
    assert!(matches!(posts[0], HookEvent::PostCommit(CommitSource::User, Some(lsn)) if lsn.0 > 0));
}

#[test]
fn p08_post_commit_not_fired_on_rejection() {
    // Uses a custom plugin that rejects pre_commit AND captures post_commit
    let post_events = Arc::new(Mutex::new(Vec::<HookEvent>::new()));
    let plugin = RejectAndCapturePlugin {
        reject_hook: "pre_commit".to_string(),
        reason: "no".to_string(),
        post_events: post_events.clone(),
    };
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    let p = HashMap::new();
    db.execute("CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)", &p)
        .unwrap();

    let id = Uuid::new_v4();
    let result = db.execute(
        "INSERT INTO items (id, name) VALUES ($id, 'widget')",
        &HashMap::from([("id".to_string(), Value::Uuid(id))]),
    );
    // Guard: pre_commit must have actually rejected (prevents vacuous pass if hooks never wired)
    assert!(
        result.is_err(),
        "INSERT must fail because pre_commit rejects"
    );

    let ev = post_events.lock().unwrap();
    assert!(
        ev.is_empty(),
        "post_commit must NOT fire when pre_commit rejects"
    );
}

#[test]
fn p09_on_open_rejects() {
    let plugin = RejectingPlugin::new("on_open", "license-expired");
    let result = Database::open_memory_with_plugin(Arc::new(plugin));
    assert!(result.is_err());
    assert!(
        matches!(result.unwrap_err(), Error::PluginRejected { ref hook, ref reason }
        if hook == "on_open" && reason == "license-expired")
    );
}

#[test]
fn p10_on_close_fires() {
    let (plugin, events) = CapturingPlugin::new();
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    db.close().unwrap();

    let ev = events.lock().unwrap();
    assert_eq!(
        ev.iter()
            .filter(|e| matches!(e, HookEvent::OnClose))
            .count(),
        1,
        "on_close must fire exactly once"
    );
    // Verify on_close appears after on_open in the event vec
    let open_idx = ev.iter().position(|e| matches!(e, HookEvent::OnOpen));
    let close_idx = ev.iter().position(|e| matches!(e, HookEvent::OnClose));
    assert!(open_idx.is_some() && close_idx.is_some());
    assert!(
        open_idx.unwrap() < close_idx.unwrap(),
        "on_close must fire after on_open"
    );
}

#[test]
fn p11_on_close_not_called_from_drop() {
    let (plugin, events) = CapturingPlugin::new();
    {
        let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
        // Guard: on_open must have fired (proves plugin is actually wired)
        let ev = events.lock().unwrap();
        assert!(
            ev.iter().any(|e| matches!(e, HookEvent::OnOpen)),
            "on_open must fire — proves plugin is wired, not vacuously empty"
        );
        drop(ev);
        drop(db); // db drops here without close()
    }
    let ev = events.lock().unwrap();
    assert!(
        !ev.iter().any(|e| matches!(e, HookEvent::OnClose)),
        "on_close must NOT fire from Drop"
    );
}

#[test]
fn p12_on_query_fires_for_dml_and_ddl() {
    let (plugin, events) = CapturingPlugin::new();
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    let p = HashMap::new();
    let id = Uuid::new_v4();
    let id_params = HashMap::from([("id".to_string(), Value::Uuid(id))]);

    // 5 statements in order: CREATE, INSERT, SELECT, UPDATE, DELETE
    db.execute("CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)", &p)
        .unwrap();
    db.execute(
        "INSERT INTO items (id, name) VALUES ($id, 'alpha')",
        &id_params,
    )
    .unwrap();
    db.execute("SELECT * FROM items", &p).unwrap();
    db.execute("UPDATE items SET name = 'beta' WHERE id = $id", &id_params)
        .unwrap();
    db.execute("DELETE FROM items WHERE id = $id", &id_params)
        .unwrap();

    let ev = events.lock().unwrap();
    let queries: Vec<_> = ev
        .iter()
        .filter_map(|e| match e {
            HookEvent::OnQuery(sql) => Some(sql.to_uppercase()),
            _ => None,
        })
        .collect();
    assert_eq!(
        queries.len(),
        5,
        "on_query must fire for all 5 statements; got {}",
        queries.len()
    );
    assert!(
        queries[0].contains("CREATE TABLE"),
        "queries[0] must be CREATE TABLE"
    );
    assert!(queries[1].contains("INSERT"), "queries[1] must be INSERT");
    assert!(queries[2].contains("SELECT"), "queries[2] must be SELECT");
    assert!(queries[3].contains("UPDATE"), "queries[3] must be UPDATE");
    assert!(queries[4].contains("DELETE"), "queries[4] must be DELETE");
}

#[test]
fn p13_on_query_skips_tx_control() {
    let (plugin, events) = CapturingPlugin::new();
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    let p = HashMap::new();
    db.execute("CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)", &p)
        .unwrap();
    db.execute("BEGIN", &p).unwrap();
    db.execute("COMMIT", &p).unwrap();

    let ev = events.lock().unwrap();
    let queries: Vec<_> = ev
        .iter()
        .filter_map(|e| match e {
            HookEvent::OnQuery(sql) => Some(sql.to_uppercase()),
            _ => None,
        })
        .collect();
    // Positive guard: on_query DID fire for CREATE TABLE (prevents vacuous pass)
    assert!(
        !queries.is_empty(),
        "on_query must fire for at least CREATE TABLE"
    );
    assert!(
        queries.iter().any(|q| q.contains("CREATE TABLE")),
        "on_query must fire for CREATE TABLE"
    );
    // Negative: tx control excluded
    assert!(
        !queries
            .iter()
            .any(|q| q.contains("BEGIN") || q.contains("COMMIT") || q.contains("ROLLBACK")),
        "on_query must NOT fire for BEGIN/COMMIT/ROLLBACK"
    );
}

#[test]
fn p14_on_ddl_fires_create_table() {
    let (plugin, events) = CapturingPlugin::new();
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    let p = HashMap::new();
    db.execute("CREATE TABLE widgets (id UUID PRIMARY KEY, label TEXT)", &p)
        .unwrap();

    let ev = events.lock().unwrap();
    let ddls: Vec<_> = ev
        .iter()
        .filter_map(|e| match e {
            HookEvent::OnDdl(desc) => Some(desc.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        ddls.len(),
        1,
        "on_ddl must fire exactly once for CREATE TABLE"
    );
    // Verify DdlChange content includes table name and columns
    assert!(
        ddls[0].contains("widgets"),
        "DdlChange must contain table name 'widgets'"
    );
    assert!(
        ddls[0].contains("id") && ddls[0].contains("label"),
        "DdlChange must contain column names"
    );
}

#[test]
fn p15_on_ddl_fires_drop_table() {
    let ddl_log: Arc<Mutex<Vec<DdlChange>>> = Arc::new(Mutex::new(Vec::new()));
    let plugin = DdlCapturingPlugin {
        changes: ddl_log.clone(),
    };
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    let p = HashMap::new();
    db.execute("CREATE TABLE widgets (id UUID PRIMARY KEY, label TEXT)", &p)
        .unwrap();

    // Verify CREATE TABLE fired on_ddl
    {
        let log = ddl_log.lock().unwrap();
        assert_eq!(log.len(), 1, "on_ddl must fire once for CREATE TABLE");
        match &log[0] {
            DdlChange::CreateTable { name, columns, .. } => {
                assert_eq!(name, "widgets");
                let col_names: Vec<&str> = columns.iter().map(|(n, _)| n.as_str()).collect();
                assert!(col_names.contains(&"id"), "must contain column 'id'");
                assert!(col_names.contains(&"label"), "must contain column 'label'");
            }
            other => panic!("expected CreateTable, got {other:?}"),
        }
    }

    db.execute("DROP TABLE widgets", &p).unwrap();

    // Verify DROP TABLE fired on_ddl
    let log = ddl_log.lock().unwrap();
    assert_eq!(log.len(), 2, "on_ddl must fire for both CREATE and DROP");
    match &log[1] {
        DdlChange::DropTable { name } => {
            assert_eq!(name, "widgets", "DropTable must reference 'widgets'");
        }
        other => panic!("expected DropTable, got {other:?}"),
    }
}

#[test]
fn p15b_on_ddl_fires_alter_table_add_column() {
    let ddl_log: Arc<Mutex<Vec<DdlChange>>> = Arc::new(Mutex::new(Vec::new()));
    let plugin = DdlCapturingPlugin {
        changes: ddl_log.clone(),
    };
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    let p = HashMap::new();
    db.execute("CREATE TABLE people (id UUID PRIMARY KEY, name TEXT)", &p)
        .unwrap();

    // Clear so we only see ALTER
    ddl_log.lock().unwrap().clear();

    db.execute("ALTER TABLE people ADD COLUMN age INTEGER", &p)
        .unwrap();

    let log = ddl_log.lock().unwrap();
    assert_eq!(
        log.len(),
        1,
        "on_ddl must fire exactly once for ALTER TABLE ADD COLUMN"
    );
    match &log[0] {
        DdlChange::AlterTable { name, columns, .. } => {
            assert_eq!(name, "people", "AlterTable must reference 'people'");
            // After ADD COLUMN age, the columns list should contain 'age'
            let col_names: Vec<&str> = columns.iter().map(|(n, _)| n.as_str()).collect();
            assert!(
                col_names.contains(&"age"),
                "AlterTable columns must include added column 'age', got: {col_names:?}"
            );
        }
        other => panic!("expected AlterTable, got {other:?}"),
    }
}

#[test]
fn p15c_on_ddl_fires_alter_table_drop_column() {
    let ddl_log: Arc<Mutex<Vec<DdlChange>>> = Arc::new(Mutex::new(Vec::new()));
    let plugin = DdlCapturingPlugin {
        changes: ddl_log.clone(),
    };
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    let p = HashMap::new();
    db.execute(
        "CREATE TABLE people (id UUID PRIMARY KEY, name TEXT, age INTEGER)",
        &p,
    )
    .unwrap();

    ddl_log.lock().unwrap().clear();

    db.execute("ALTER TABLE people DROP COLUMN age", &p)
        .unwrap();

    let log = ddl_log.lock().unwrap();
    assert_eq!(
        log.len(),
        1,
        "on_ddl must fire exactly once for ALTER TABLE DROP COLUMN"
    );
    match &log[0] {
        DdlChange::AlterTable { name, columns, .. } => {
            assert_eq!(name, "people", "AlterTable must reference 'people'");
            // After DROP COLUMN age, the columns list should reflect the
            // post-alteration schema (id, name) — 'age' should be absent.
            let col_names: Vec<&str> = columns.iter().map(|(n, _)| n.as_str()).collect();
            assert!(
                !col_names.contains(&"age"),
                "AlterTable columns must NOT include dropped column 'age', got: {col_names:?}"
            );
            assert!(
                col_names.contains(&"name"),
                "AlterTable columns must still include 'name', got: {col_names:?}"
            );
        }
        other => panic!("expected AlterTable, got {other:?}"),
    }
}

#[test]
fn p15d_on_ddl_fires_alter_table_rename_column() {
    let ddl_log: Arc<Mutex<Vec<DdlChange>>> = Arc::new(Mutex::new(Vec::new()));
    let plugin = DdlCapturingPlugin {
        changes: ddl_log.clone(),
    };
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    let p = HashMap::new();
    db.execute("CREATE TABLE people (id UUID PRIMARY KEY, name TEXT)", &p)
        .unwrap();

    ddl_log.lock().unwrap().clear();

    db.execute("ALTER TABLE people RENAME COLUMN name TO full_name", &p)
        .unwrap();

    let log = ddl_log.lock().unwrap();
    assert_eq!(
        log.len(),
        1,
        "on_ddl must fire exactly once for ALTER TABLE RENAME COLUMN"
    );
    match &log[0] {
        DdlChange::AlterTable { name, columns, .. } => {
            assert_eq!(name, "people", "AlterTable must reference 'people'");
            // After RENAME COLUMN name -> full_name, columns should contain
            // 'full_name' and not 'name'.
            let col_names: Vec<&str> = columns.iter().map(|(n, _)| n.as_str()).collect();
            assert!(
                col_names.contains(&"full_name"),
                "AlterTable columns must include renamed column 'full_name', got: {col_names:?}"
            );
            assert!(
                !col_names.contains(&"name"),
                "AlterTable columns must NOT include old name 'name' after rename, got: {col_names:?}"
            );
        }
        other => panic!("expected AlterTable, got {other:?}"),
    }
}

#[test]
fn p15e_on_ddl_veto_drop_table() {
    let plugin = DdlVetoPlugin {
        reject_variant: "DropTable".to_string(),
    };
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    let p = HashMap::new();

    // CREATE TABLE is AlterTable/CreateTable — not vetoed by this plugin
    // (plugin only rejects DropTable)
    db.execute("CREATE TABLE widgets (id UUID PRIMARY KEY, label TEXT)", &p)
        .unwrap();

    let result = db.execute("DROP TABLE widgets", &p);
    assert!(
        result.is_err(),
        "DROP TABLE must be rejected when plugin vetoes DropTable"
    );
    let err = result.unwrap_err();
    assert!(
        matches!(err, Error::PluginRejected { ref hook, .. } if hook == "on_ddl"),
        "error must be PluginRejected at on_ddl hook, got: {err:?}"
    );

    // Table must still exist after veto
    assert!(
        db.table_names().contains(&"widgets".to_string()),
        "table 'widgets' must still exist after vetoed DROP"
    );

    // Verify we can still query the table
    let result = db.execute("SELECT * FROM widgets", &p).unwrap();
    assert_eq!(
        result.rows.len(),
        0,
        "table must be queryable after vetoed DROP"
    );
}

#[test]
fn p15f_on_ddl_veto_alter_table() {
    let plugin = DdlVetoPlugin {
        reject_variant: "AlterTable".to_string(),
    };
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    let p = HashMap::new();

    db.execute("CREATE TABLE people (id UUID PRIMARY KEY, name TEXT)", &p)
        .unwrap();

    let result = db.execute("ALTER TABLE people ADD COLUMN age INTEGER", &p);
    assert!(
        result.is_err(),
        "ALTER TABLE must be rejected when plugin vetoes AlterTable"
    );
    let err = result.unwrap_err();
    assert!(
        matches!(err, Error::PluginRejected { ref hook, .. } if hook == "on_ddl"),
        "error must be PluginRejected at on_ddl hook, got: {err:?}"
    );

    // Schema must be unchanged — inserting with 'age' column should fail
    let insert_result = db.execute(
        "INSERT INTO people (id, name, age) VALUES ('00000000-0000-0000-0000-000000000001', 'alice', 30)",
        &p,
    );
    assert!(
        insert_result.is_err(),
        "insert with 'age' column must fail because ALTER was vetoed"
    );

    // Original schema must work
    db.execute(
        "INSERT INTO people (id, name) VALUES ('00000000-0000-0000-0000-000000000002', 'bob')",
        &p,
    )
    .unwrap();
    let result = db.execute("SELECT * FROM people", &p).unwrap();
    assert_eq!(
        result.columns.len(),
        2,
        "schema must still have exactly 2 columns (id, name)"
    );
}

#[test]
fn p16_on_ddl_vetoes_create_table() {
    let plugin = RejectingPlugin::new("on_ddl", "schema-frozen");
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    let p = HashMap::new();
    let result = db.execute("CREATE TABLE widgets (id UUID PRIMARY KEY)", &p);
    assert!(result.is_err());
    assert!(
        matches!(result.unwrap_err(), Error::PluginRejected { ref hook, ref reason }
        if hook == "on_ddl" && reason == "schema-frozen")
    );
    assert!(!db.table_names().contains(&"widgets".to_string()));
}

#[test]
fn p17_on_query_before_on_ddl() {
    let (plugin, events) = CapturingPlugin::new();
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    let p = HashMap::new();
    db.execute("CREATE TABLE widgets (id UUID PRIMARY KEY, label TEXT)", &p)
        .unwrap();

    let ev = events.lock().unwrap();
    let query_idx = ev.iter().position(|e| matches!(e, HookEvent::OnQuery(_)));
    let ddl_idx = ev.iter().position(|e| matches!(e, HookEvent::OnDdl(_)));
    assert!(query_idx.is_some() && ddl_idx.is_some());
    assert!(
        query_idx.unwrap() < ddl_idx.unwrap(),
        "on_query must fire before on_ddl for DDL statements"
    );
}

#[test]
fn p18_post_query_success_outcome() {
    let (plugin, events) = CapturingPlugin::new();
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    let p = HashMap::new();
    db.execute("CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)", &p)
        .unwrap();
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO items (id, name) VALUES ($id, 'a')",
        &HashMap::from([("id".to_string(), Value::Uuid(id))]),
    )
    .unwrap();
    db.execute("SELECT * FROM items", &p).unwrap();

    let ev = events.lock().unwrap();
    let pqs: Vec<_> = ev
        .iter()
        .filter_map(|e| match e {
            HookEvent::PostQuery(sql, outcome) => Some((sql.clone(), outcome.clone())),
            _ => None,
        })
        .collect();
    assert!(!pqs.is_empty(), "post_query must fire");
    // Find the SELECT post_query
    let select_pq = pqs
        .iter()
        .find(|(sql, _)| sql.to_uppercase().contains("SELECT"));
    assert!(select_pq.is_some(), "post_query must fire for SELECT");
    assert!(
        matches!(&select_pq.unwrap().1, QueryOutcome::Success { row_count } if *row_count == 1)
    );
}

#[test]
fn p19_post_query_error_outcome() {
    let (plugin, events) = CapturingPlugin::new();
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    let p = HashMap::new();
    db.execute("CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)", &p)
        .unwrap();

    // Insert a row, then insert a duplicate PK to trigger execution-time error.
    // The error must come from execution (after on_query fires), not from
    // planning/validation (which would fire before on_query).
    let id = Uuid::new_v4();
    let id_params = HashMap::from([("id".to_string(), Value::Uuid(id))]);
    db.execute(
        "INSERT INTO items (id, name) VALUES ($id, 'first')",
        &id_params,
    )
    .unwrap();
    let result = db.execute(
        "INSERT INTO items (id, name) VALUES ($id, 'duplicate')",
        &id_params,
    );
    assert!(result.is_err(), "duplicate PK must produce an error");

    let ev = events.lock().unwrap();
    let post_queries: Vec<_> = ev
        .iter()
        .filter_map(|e| match e {
            HookEvent::PostQuery(sql, outcome) => Some((sql.clone(), outcome.clone())),
            _ => None,
        })
        .collect();
    // Find the duplicate INSERT's post_query — it should have Error outcome
    let error_pqs: Vec<_> = post_queries
        .iter()
        .filter(|(_, o)| matches!(o, QueryOutcome::Error { .. }))
        .collect();
    assert!(
        !error_pqs.is_empty(),
        "post_query must fire with Error outcome for duplicate PK insert"
    );
    assert!(
        error_pqs[0].0.to_uppercase().contains("INSERT"),
        "error post_query must be for the INSERT statement"
    );
}

#[test]
fn p20_post_query_skips_tx_control() {
    let (plugin, events) = CapturingPlugin::new();
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    let p = HashMap::new();
    // First: a CREATE TABLE to prove post_query DOES fire (positive guard)
    db.execute("CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)", &p)
        .unwrap();

    let ev = events.lock().unwrap();
    let pqs_before: Vec<_> = ev
        .iter()
        .filter(|e| matches!(e, HookEvent::PostQuery(..)))
        .collect();
    assert!(
        !pqs_before.is_empty(),
        "post_query must fire for CREATE TABLE (positive guard)"
    );
    let count_before = pqs_before.len();
    drop(ev);

    // Now: tx control statements — post_query count must not increase
    db.execute("BEGIN", &p).unwrap();
    db.execute("COMMIT", &p).unwrap();

    let ev = events.lock().unwrap();
    let pqs_after: Vec<_> = ev
        .iter()
        .filter(|e| matches!(e, HookEvent::PostQuery(..)))
        .collect();
    assert_eq!(
        pqs_after.len(),
        count_before,
        "post_query must NOT fire for BEGIN/COMMIT — count must not increase"
    );
}

#[test]
fn p21_post_query_skipped_when_on_query_rejects() {
    let (plugin, post_calls) = SelectiveRejectPlugin::new("INSERT");
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    let p = HashMap::new();
    // CREATE TABLE goes through (on_query doesn't reject it)
    db.execute("CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)", &p)
        .unwrap();

    // Positive guard: post_query must have fired for CREATE TABLE (proves hooks are wired)
    {
        let calls = post_calls.lock().unwrap();
        assert!(
            !calls.is_empty(),
            "post_query must fire for CREATE TABLE (positive guard — prevents vacuous pass)"
        );
    }

    // INSERT gets rejected by on_query
    let _ = db.execute(
        "INSERT INTO items (id, name) VALUES ($id, 'a')",
        &HashMap::from([("id".to_string(), Value::Uuid(Uuid::new_v4()))]),
    );

    let calls = post_calls.lock().unwrap();
    // post_query should have fired for CREATE TABLE but NOT for the rejected INSERT
    assert!(
        !calls
            .iter()
            .any(|sql| sql.to_uppercase().contains("INSERT")),
        "post_query must NOT fire for queries rejected by on_query"
    );
}

#[test]
fn p22_post_query_skipped_when_on_ddl_rejects() {
    // Plugin that rejects on_ddl and captures post_query
    let post_events = Arc::new(Mutex::new(Vec::<String>::new()));
    let plugin = DdlRejectWithPostCapture {
        reason: "frozen".to_string(),
        post_query_calls: post_events.clone(),
    };
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    let p = HashMap::new();
    let result = db.execute("CREATE TABLE widgets (id UUID PRIMARY KEY)", &p);
    assert!(
        result.is_err(),
        "CREATE TABLE must fail because on_ddl rejects"
    );
    assert!(
        matches!(result.unwrap_err(), Error::PluginRejected { ref hook, .. }
        if hook == "on_ddl")
    );

    let calls = post_events.lock().unwrap();
    assert!(
        calls.is_empty(),
        "post_query must NOT fire when on_ddl rejects"
    );
}

#[test]
fn p23_on_sync_push_filters_rows() {
    let plugin = FilterPlugin::new("secret_items");
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    let p = HashMap::new();
    db.execute("CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)", &p)
        .unwrap();
    db.execute(
        "CREATE TABLE secret_items (id UUID PRIMARY KEY, data TEXT)",
        &p,
    )
    .unwrap();
    let id1 = Uuid::new_v4();
    let id2 = Uuid::new_v4();
    db.execute(
        "INSERT INTO items (id, name) VALUES ($id, 'public')",
        &HashMap::from([("id".to_string(), Value::Uuid(id1))]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO secret_items (id, data) VALUES ($id, 'classified')",
        &HashMap::from([("id".to_string(), Value::Uuid(id2))]),
    )
    .unwrap();

    // Get changeset and call on_sync_push directly on the plugin
    // (tests plugin behavior, not the SyncClient transport path)
    let mut cs = db.changes_since(Lsn(0));
    assert!(
        cs.rows.iter().any(|r| r.table == "secret_items"),
        "changeset must contain secret_items BEFORE on_sync_push"
    );
    db.plugin().on_sync_push(&mut cs).unwrap();
    assert!(
        cs.rows.iter().all(|r| r.table != "secret_items"),
        "on_sync_push must have filtered out secret_items rows"
    );
    assert!(
        cs.rows.iter().any(|r| r.table == "items"),
        "on_sync_push must preserve non-filtered rows"
    );
}

#[test]
fn p24_on_sync_pull_rejects_batch() {
    let plugin = RejectingPlugin::new("on_sync_pull", "quarantined");
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    let p = HashMap::new();
    db.execute("CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)", &p)
        .unwrap();

    let cs = ChangeSet {
        rows: vec![RowChange {
            table: "items".to_string(),
            natural_key: NaturalKey {
                column: "id".to_string(),
                value: Value::Uuid(Uuid::new_v4()),
            },
            values: HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("name".to_string(), Value::Text("incoming".to_string())),
            ]),
            deleted: false,
            lsn: Lsn(1),
        }],
        edges: vec![],
        vectors: vec![],
        ddl: vec![],
    };
    let result = db.apply_changes(cs, &ConflictPolicies::uniform(ConflictPolicy::LatestWins));
    assert!(result.is_err());
    assert!(
        matches!(result.unwrap_err(), Error::PluginRejected { ref hook, ref reason }
        if hook == "on_sync_pull" && reason == "quarantined")
    );
    assert!(db.scan("items", db.snapshot()).unwrap().is_empty());
}

#[test]
fn p25_on_sync_pull_filters_rows() {
    let plugin = FilterPlugin::new("blocked");
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    let p = HashMap::new();
    db.execute("CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)", &p)
        .unwrap();
    db.execute("CREATE TABLE blocked (id UUID PRIMARY KEY, data TEXT)", &p)
        .unwrap();

    let id1 = Uuid::new_v4();
    let id2 = Uuid::new_v4();
    let cs = ChangeSet {
        rows: vec![
            RowChange {
                table: "items".to_string(),
                natural_key: NaturalKey {
                    column: "id".to_string(),
                    value: Value::Uuid(id1),
                },
                values: HashMap::from([
                    ("id".to_string(), Value::Uuid(id1)),
                    ("name".to_string(), Value::Text("allowed".to_string())),
                ]),
                deleted: false,
                lsn: Lsn(1),
            },
            RowChange {
                table: "blocked".to_string(),
                natural_key: NaturalKey {
                    column: "id".to_string(),
                    value: Value::Uuid(id2),
                },
                values: HashMap::from([
                    ("id".to_string(), Value::Uuid(id2)),
                    ("data".to_string(), Value::Text("nope".to_string())),
                ]),
                deleted: false,
                lsn: Lsn(2),
            },
        ],
        edges: vec![],
        vectors: vec![],
        ddl: vec![],
    };
    db.apply_changes(cs, &ConflictPolicies::uniform(ConflictPolicy::LatestWins))
        .unwrap();

    assert_eq!(db.scan("items", db.snapshot()).unwrap().len(), 1);
    assert!(
        db.scan("blocked", db.snapshot()).unwrap().is_empty(),
        "blocked table rows must be filtered by on_sync_pull"
    );
}

#[test]
fn p26_health_custom_status() {
    // Plugin returns Degraded
    let plugin = CustomHealthPlugin;
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    assert_eq!(
        db.plugin_health(),
        PluginHealth::Degraded("disk-pressure".to_string())
    );
}

#[test]
fn p27_describe_custom_json() {
    let plugin = CustomDescribePlugin;
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    let desc = db.plugin_describe();
    assert_eq!(desc["name"], "audit-plugin");
    assert_eq!(desc["version"], "1.0");
}

#[test]
fn p28_decorator_inner_before_outer_pre_commit() {
    let log = Arc::new(Mutex::new(Vec::<String>::new()));
    let inner = OrderingPlugin::new("inner", log.clone());
    let outer = OrderingPlugin::new("outer", log.clone());
    // Compose: outer wraps inner
    let composed = DecoratorPlugin::new(Box::new(outer), Box::new(inner));
    let db = Database::open_memory_with_plugin(Arc::new(composed)).unwrap();
    let p = HashMap::new();
    db.execute("CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)", &p)
        .unwrap();
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO items (id, name) VALUES ($id, 'a')",
        &HashMap::from([("id".to_string(), Value::Uuid(id))]),
    )
    .unwrap();

    let entries = log.lock().unwrap();
    let pre_commits: Vec<_> = entries
        .iter()
        .filter(|e| e.contains("pre_commit"))
        .collect();
    assert_eq!(pre_commits.len(), 2);
    assert_eq!(pre_commits[0], "inner:pre_commit");
    assert_eq!(pre_commits[1], "outer:pre_commit");
}

#[test]
fn p29_decorator_inner_before_outer_on_query() {
    let log = Arc::new(Mutex::new(Vec::<String>::new()));
    let inner = OrderingPlugin::new("inner", log.clone());
    let outer = OrderingPlugin::new("outer", log.clone());
    let composed = DecoratorPlugin::new(Box::new(outer), Box::new(inner));
    let db = Database::open_memory_with_plugin(Arc::new(composed)).unwrap();
    let p = HashMap::new();
    db.execute("CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)", &p)
        .unwrap();

    let entries = log.lock().unwrap();
    let on_queries: Vec<_> = entries.iter().filter(|e| e.contains("on_query")).collect();
    assert!(on_queries.len() >= 2);
    assert!(on_queries[0].starts_with("inner:"), "inner must fire first");
    assert!(
        on_queries[1].starts_with("outer:"),
        "outer must fire second"
    );
}

// P30 removed — Display impl check merged into P01

#[test]
fn p31_open_memory_returns_self() {
    let db = Database::open_memory(); // Must compile without ? or unwrap
    assert!(db.table_names().is_empty());
}

// P32 is implicit — running `cargo test --workspace` covers it.

#[test]
fn p33_multiple_autocommits_fire_separate_pairs() {
    let (plugin, events) = CapturingPlugin::new();
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    let p = HashMap::new();
    db.execute("CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)", &p)
        .unwrap();

    for i in 0..3 {
        let id = Uuid::new_v4();
        db.execute(
            "INSERT INTO items (id, name) VALUES ($id, $name)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(id)),
                ("name".to_string(), Value::Text(format!("item-{i}"))),
            ]),
        )
        .unwrap();
    }

    let ev = events.lock().unwrap();
    let pres: Vec<_> = ev
        .iter()
        .filter(|e| matches!(e, HookEvent::PreCommit(CommitSource::AutoCommit, _)))
        .collect();
    let posts: Vec<_> = ev
        .iter()
        .filter(|e| matches!(e, HookEvent::PostCommit(CommitSource::AutoCommit, _)))
        .collect();
    assert_eq!(
        pres.len(),
        3,
        "3 autocommit INSERTs must produce 3 pre_commit calls"
    );
    assert_eq!(
        posts.len(),
        3,
        "3 autocommit INSERTs must produce 3 post_commit calls"
    );

    // Verify strict interleaving: pre/post/pre/post/pre/post
    let commit_events: Vec<&str> = ev
        .iter()
        .filter_map(|e| match e {
            HookEvent::PreCommit(CommitSource::AutoCommit, _) => Some("pre"),
            HookEvent::PostCommit(CommitSource::AutoCommit, _) => Some("post"),
            _ => None,
        })
        .collect();
    assert_eq!(
        commit_events,
        vec!["pre", "post", "pre", "post", "pre", "post"],
        "commit hooks must interleave strictly: pre/post for each autocommit"
    );
}

#[test]
fn p34_apply_changes_per_row_sync_pull() {
    let (plugin, events) = CapturingPlugin::new();
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    let p = HashMap::new();
    db.execute("CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)", &p)
        .unwrap();

    let rows: Vec<_> = (0..3)
        .map(|i| {
            let id = Uuid::new_v4();
            RowChange {
                table: "items".to_string(),
                natural_key: NaturalKey {
                    column: "id".to_string(),
                    value: Value::Uuid(id),
                },
                values: HashMap::from([
                    ("id".to_string(), Value::Uuid(id)),
                    ("name".to_string(), Value::Text(format!("sync-{i}"))),
                ]),
                deleted: false,
                lsn: Lsn((i + 1) as u64),
            }
        })
        .collect();

    let cs = ChangeSet {
        rows,
        edges: vec![],
        vectors: vec![],
        ddl: vec![],
    };
    db.apply_changes(cs, &ConflictPolicies::uniform(ConflictPolicy::LatestWins))
        .unwrap();

    let ev = events.lock().unwrap();
    let sync_pres: Vec<_> = ev
        .iter()
        .filter(|e| matches!(e, HookEvent::PreCommit(CommitSource::SyncPull, _)))
        .collect();
    assert!(
        sync_pres.len() >= 3,
        "apply_changes must fire pre_commit per row with SyncPull; got {}",
        sync_pres.len()
    );
    let sync_posts: Vec<_> = ev
        .iter()
        .filter(|e| matches!(e, HookEvent::PostCommit(CommitSource::SyncPull, _)))
        .collect();
    assert!(
        sync_posts.len() >= 3,
        "apply_changes must fire post_commit per row with SyncPull; got {}",
        sync_posts.len()
    );
}

#[test]
fn p35_on_query_vetoes_dml() {
    let (plugin, _) = SelectiveRejectPlugin::new("DELETE");
    let db = Database::open_memory_with_plugin(Arc::new(plugin)).unwrap();
    let p = HashMap::new();
    db.execute("CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)", &p)
        .unwrap();
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO items (id, name) VALUES ($id, 'keeper')",
        &HashMap::from([("id".to_string(), Value::Uuid(id))]),
    )
    .unwrap();

    let result = db.execute(
        "DELETE FROM items WHERE id = $id",
        &HashMap::from([("id".to_string(), Value::Uuid(id))]),
    );
    assert!(result.is_err());
    assert_eq!(
        db.scan("items", db.snapshot()).unwrap().len(),
        1,
        "Row must survive because on_query vetoed the DELETE"
    );
}
