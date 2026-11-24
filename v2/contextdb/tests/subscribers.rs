//! Extends events.rs's commit outcomes; the absent plug-in/subscriber API is isolated here.
mod support;
use contextdb::{
    ChangeSet, CommitEvent, CommitSource, DatabaseOpenOptions, DatabasePlugin, Error, Result,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering::SeqCst},
    mpsc::{Receiver, TryRecvError::Empty},
};
use support::*;
struct Probe(
    &'static str,
    Mutex<Vec<Receiver<CommitEvent>>>,
    AtomicU64,
    Mutex<Vec<String>>,
);
impl DatabasePlugin for Probe {
    fn pre_commit(&self, changes: &ChangeSet, _: CommitSource) -> Result<()> {
        assert!(!changes.is_empty());
        if self.0 == "veto" {
            return Err(Error::PlanError("host veto".into()));
        }
        Ok(())
    }
    fn post_commit(&self, _: &ChangeSet, _: CommitSource) {
        self.2.fetch_add(1, SeqCst);
    }
    fn commit_failed(&self, _: &ChangeSet, _: CommitSource, error: &Error) {
        self.3.lock().unwrap().push(error.to_string());
    }
}
#[test]
fn a_plug_in_refusal_leaves_nothing_and_subscribers_hear_only_after_disk_confirms() {
    for outcome in ["commit", "veto"] {
        let folder = folder(&format!("observers-{outcome}"));
        let db = open(&folder);
        run(&db, "CREATE TABLE notes (id INT PRIMARY KEY)");
        close(db);
        let probe = Arc::new(Probe(
            outcome,
            Mutex::default(),
            AtomicU64::new(0),
            Mutex::default(),
        ));
        let watching = probe.clone();
        let armed = Arc::new(AtomicBool::new(true));
        let checking = armed.clone();
        let mut options = DatabaseOpenOptions::default();
        options.plugin = Some(probe.clone());
        options.storage = Some(storage(move |call| {
            if checking.load(SeqCst) && matches!(call, Call::Write(..) | Call::Sync) {
                assert_eq!(watching.2.load(SeqCst), 0);
                for rx in watching.1.lock().unwrap().iter() {
                    assert_eq!(rx.try_recv(), Err(Empty));
                }
            }
        }));
        let db = open_with(&folder, options).unwrap();
        probe.1.lock().unwrap().push(db.subscribe().unwrap());
        let bounded = db.subscribe_with_capacity(1).unwrap();
        probe.1.lock().unwrap().push(bounded);
        let before = watermark(&db);
        let tx = db.transaction().unwrap();
        run(&tx, "INSERT INTO notes VALUES (1), (2)");
        for rx in probe.1.lock().unwrap().iter() {
            assert_eq!(rx.try_recv(), Err(Empty));
        }
        let result = tx.commit();
        armed.store(false, SeqCst);
        assert_eq!(result.is_ok(), outcome == "commit");
        if outcome == "veto" {
            assert!(matches!(result, Err(Error::PlanError(s)) if s == "host veto"));
            let failed = probe.3.lock().unwrap();
            assert!(
                failed[0].contains("host veto"),
                "the hook gets the refusal: {failed:?}"
            );
        }
        assert_eq!(probe.2.load(SeqCst), u64::from(outcome == "commit"));
        for rx in probe.1.lock().unwrap().iter() {
            if outcome == "commit" {
                let event = rx.try_recv().expect("confirmed commit");
                assert_eq!(event.source, CommitSource::User);
                assert_eq!(event.lsn, watermark(&db));
                assert_eq!(event.tables_changed(), ["notes"]);
                assert_eq!(event.row_count, 2);
            }
            assert_eq!(rx.try_recv(), Err(Empty));
        }
        if outcome != "commit" {
            assert_eq!(watermark(&db), before);
        }
        probe.1.lock().unwrap().clear();
        let db = reopen(db, &folder);
        assert_eq!(
            int(&db, "SELECT COUNT(*) FROM notes"),
            if outcome == "commit" { 2 } else { 0 }
        );
    }
}
struct Heard(Mutex<Vec<String>>);
impl DatabasePlugin for Heard {
    fn post_commit(&self, _: &ChangeSet, source: CommitSource) {
        self.0.lock().unwrap().push(format!("{source:?}"));
    }
    fn post_query(&self, _: &str, _: std::time::Duration, o: &contextdb::QueryOutcome) {
        self.0.lock().unwrap().push(format!("{o:?}"));
    }
    fn on_query(&self, _: &str) -> Result<()> {
        self.0.lock().unwrap().push("Query".into());
        Ok(())
    }
    fn on_ddl(&self, _: &str) -> Result<()> {
        self.0.lock().unwrap().push("Ddl".into());
        Ok(())
    }
    fn health(&self) -> contextdb::PluginHealth {
        contextdb::PluginHealth::Degraded("heard".into())
    }
    fn describe(&self) -> serde_json::Value {
        serde_json::json!({"heard": true})
    }
}
#[test]
fn a_plug_in_hears_statements_and_commit_sources_while_a_full_subscriber_only_loses_events() {
    let heard = Arc::new(Heard(Mutex::default()));
    let mut options = DatabaseOpenOptions::default();
    options.plugin = Some(heard.clone());
    let db = open_with(&folder("observers-heard"), options).unwrap();
    run(&db, "CREATE TABLE notes (id INT PRIMARY KEY)");
    let rx = db.subscribe_with_capacity(1).unwrap();
    let (tx, insert) = (db.transaction().unwrap(), "INSERT INTO notes VALUES (1)");
    let _ = (run(&tx, insert), refusal(&tx, insert));
    tx.commit().unwrap();
    run(&db, "INSERT INTO notes VALUES (2)");
    let log = heard.0.lock().unwrap();
    let kinds: Vec<_> = log.iter().map(|l| l.split(' ').next().unwrap()).collect();
    let want =
        "Query Ddl AutoCommit Success Query Success Query Error User Query AutoCommit Success";
    assert_eq!(
        kinds.join(" "),
        want,
        "outcomes in a transaction, commit sources"
    );
    assert_eq!(rx.try_recv().unwrap().row_count, 1, "first event kept");
    assert_eq!(rx.try_recv().unwrap_err(), Empty, "later events dropped");
    let health = contextdb::PluginHealth::Degraded("heard".into());
    assert_eq!(
        (db.plugin_health(), db.plugin_describe()),
        (health, serde_json::json!({"heard": true}))
    );
    // Under memory pressure the write pays for its own notice: the commit stands and is heard.
    let db = open_limited(&folder("observers-summary"), 16 << 20);
    run(&db, "CREATE TABLE notes (id INT PRIMARY KEY)");
    let full = db.subscribe_with_capacity(1).unwrap();
    run(&db, "INSERT INTO notes VALUES (1)");
    let tx = db.transaction().unwrap();
    run(&tx, "INSERT INTO notes VALUES (2)");
    let (free, slot) = (
        limit(&db).unwrap() - used(&db),
        size_of::<CommitEvent>() + 64,
    );
    let first = db.subscribe_with_capacity((free - 128) / slot).unwrap();
    let left = limit(&db).unwrap() - used(&db);
    assert!(left < 128 + 143, "under one table's summary: {left}");
    tx.commit().expect("a notice never refuses its commit");
    let notice = first.try_recv().expect("the last commit is heard");
    assert_eq!(notice.tables_changed(), ["notes"]);
    assert_eq!(notice.lsn, watermark(&db));
    assert!(full.try_recv().unwrap().lsn < notice.lsn && full.try_recv() == Err(Empty));
    let health = db.subscription_health().unwrap();
    assert_eq!([health.events_sent, health.events_dropped], [2, 1]);
    assert_eq!(health.active_channels, 2);
    let (kept, held) = (notice.clone(), used(&db));
    drop(notice);
    assert_eq!(used(&db), held, "a copy keeps the charge");
    drop(kept);
    assert!(used(&db) < held, "the last copy gives it back");
    // The subscription above holds nearly the whole limit. Reopen so the empty notice has room.
    close(db);
    let db = open_limited(&folder("observers-summary"), 16 << 20);
    let hear = db.subscribe_with_capacity(1).unwrap();
    run(&db, "CREATE TABLE more (id INT PRIMARY KEY)");
    let empty = hear.try_recv().map(|e| (e.lsn, e.tables_changed().len()));
    assert_eq!(empty, Ok((watermark(&db), 0)), "no row, no table named");
}
