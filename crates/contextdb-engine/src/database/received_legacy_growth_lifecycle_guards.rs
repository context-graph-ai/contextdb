// Included inside received_legacy_growth_guards. These use the same funded
// production receiver and real signed unit, without modifying any stage.
use std::sync::atomic::{AtomicBool, Ordering};

struct CancelPrepared {
    reached: Arc<AtomicBool>,
    cancel: AtomicBool,
}
impl crate::plugin::DatabasePlugin for CancelPrepared {
    fn pre_commit(
        &self,
        _: &contextdb_tx::WriteSet,
        source: crate::plugin::CommitSource,
    ) -> Result<()> {
        if source == crate::plugin::CommitSource::SyncPull {
            self.reached.store(true, Ordering::SeqCst);
            if self.cancel.load(Ordering::SeqCst) {
                return Err(Error::ReadCancelled);
            }
        }
        Ok(())
    }
}
fn with_plugin(peer: JourneyPeer, plugin: Arc<dyn crate::plugin::DatabasePlugin>) -> JourneyPeer {
    peer.db.close().unwrap();
    drop(peer.db);
    let mut configuration = options(WRITER);
    configuration.plugin = plugin;
    let db = Database::open_with_options(&peer.path, configuration).unwrap();
    assert!(matches!(&db.tx_mgr, DatabaseTransactionHost::Legacy(_)));
    JourneyPeer {
        db,
        path: peer.path,
        identity: peer.identity,
    }
}
fn physical_lifecycle(operation: impl FnOnce()) {
    let (((), pages), raw_peak, raw_live, payload_peak, payload_live) =
        physical::measured_payload(|| redb::observe_page_lifetimes_for_test(operation));
    eprintln!(
        "LEGACY_LIFECYCLE raw_peak={raw_peak} payload_peak={payload_peak} raw_live={raw_live} payload_live={payload_live} pages={pages:?}"
    );
    assert!(payload_peak <= WRITER + READER);
    assert_eq!((raw_live, payload_live), (0, 0));
    assert_eq!((pages.current_pages, pages.current_bytes), (0, 0));
    assert_eq!(pages.allocated_pages, pages.freed_pages);
    assert_eq!(pages.allocated_bytes, pages.freed_bytes);
    assert!(pages.peak_bytes <= (WRITER + READER) as i128);
}
fn failed_preparation_cleanup(durable: bool) {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let (author, receiver, before, retained_at) = seeded(root.path(), N);
    let delivery = Delivery::authored_by(&author, before);
    receiver.db.with_reader_memory_high_water_for_test(|| ());
    receiver.db.close().unwrap();
    drop(receiver.db);
    let path = receiver.path;
    let identity = receiver.identity;
    physical_lifecycle(|| {
        let reached = Arc::new(AtomicBool::new(false));
        let plugin = Arc::new(CancelPrepared {
            reached: reached.clone(),
            cancel: AtomicBool::new(!durable),
        });
        let mut configuration = options(WRITER);
        configuration.plugin = plugin.clone();
        let receiver = JourneyPeer {
            db: Database::open_with_options(&path, configuration).unwrap(),
            path: path.clone(),
            identity: identity.clone(),
        };
        assert!(matches!(
            &receiver.db.tx_mgr,
            DatabaseTransactionHost::Legacy(_)
        ));
        let old_lsn = receiver.db.current_lsn();
        let old_publication = format!("{:?}", receiver.db.published_state_snapshot_for_test());
        let held = receiver.db.accountant.usage().used;
        if durable {
            receiver
                .db
                .fail_next_received_schema_durable_commit_for_test();
        }
        let (outcome, memory) = receiver
            .db
            .with_reader_memory_high_water_for_test(|| delivery.apply(&receiver));
        assert!(
            outcome.is_err(),
            "the real cancellation/durable failure refuses the unit"
        );
        if durable {
            assert!(
                matches!(&outcome, Err(error @ Error::Other(_))
                    if error.to_string().contains("injected received-schema Redb pre-commit failure")),
                "the durable apply must reach its injected pre-commit fault, not an earlier refusal: {outcome:?}"
            );
        }
        assert!(
            reached.load(Ordering::SeqCst),
            "full bounded staging must reach pre_commit before this refusal"
        );
        if !durable {
            assert!(
                matches!(&outcome, Err(Error::ReadCancelled)),
                "the real plugin returns typed caller cancellation"
            );
        }
        drop(outcome);
        assert_eq!(receiver.db.current_lsn(), old_lsn);
        assert_eq!(
            format!("{:?}", receiver.db.published_state_snapshot_for_test()),
            old_publication
        );
        assert_eq!(
            receiver.db.accountant.usage().used,
            held,
            "failed prepared stage releases all writer custody"
        );
        assert_eq!(
            memory.after, memory.before,
            "failed prepared stage releases all reader custody"
        );
        exact(&receiver.db, N, false, retained_at);
        let receiver = reopen(receiver, WRITER);
        exact(&receiver.db, N, false, retained_at);
        let plugin = Arc::new(CancelPrepared {
            reached,
            cancel: AtomicBool::new(false),
        });
        let receiver = with_plugin(receiver, plugin);
        delivery.apply(&receiver).unwrap();
        exact(&receiver.db, N, true, retained_at);
        let receiver = reopen(receiver, WRITER);
        exact(&receiver.db, N, true, retained_at);
        receiver.db.close().unwrap();
        drop(receiver);
    });
    author.db.close().unwrap();
}
#[test]
#[serial]
fn production_legacy_plugin_cancellation_releases_prepared_population_and_retries() {
    failed_preparation_cleanup(false);
}
#[test]
#[serial]
fn production_legacy_durable_failure_releases_prepared_population_and_retries() {
    failed_preparation_cleanup(true);
}

enum PublicationEvent {
    Prepared,
    Finished(bool),
}
struct ReleaseGate(std::sync::mpsc::Sender<()>);
impl Drop for ReleaseGate {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}
struct PublicationGate {
    events: std::sync::mpsc::Sender<PublicationEvent>,
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}
impl crate::plugin::DatabasePlugin for PublicationGate {
    fn pre_commit(
        &self,
        _: &contextdb_tx::WriteSet,
        source: crate::plugin::CommitSource,
    ) -> Result<()> {
        if source == crate::plugin::CommitSource::SyncPull {
            self.events.send(PublicationEvent::Prepared).unwrap();
            self.release.lock().unwrap().recv().unwrap();
        }
        Ok(())
    }
}
#[test]
#[serial]
fn production_legacy_grown_stage_remains_invisible_before_final_publication() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let (author, receiver, before, retained_at) = seeded(root.path(), N);
    let delivery = Delivery::authored_by(&author, before);
    let (events, event_reader) = std::sync::mpsc::channel();
    let (release, releases) = std::sync::mpsc::channel();
    let receiver = with_plugin(
        receiver,
        Arc::new(PublicationGate {
            events: events.clone(),
            release: std::sync::Mutex::new(releases),
        }),
    );
    let old_publication = format!("{:?}", receiver.db.published_state_snapshot_for_test());
    let old_lsn = receiver.db.current_lsn();
    std::thread::scope(|threads| {
        let _release_on_unwind = ReleaseGate(release.clone());
        let task = threads.spawn(|| {
            let applied = delivery.apply(&receiver);
            let ok = applied.is_ok();
            if let Err(error) = applied {
                eprintln!("PUBLICATION_STAGE_REFUSAL {error:?}");
            }
            events.send(PublicationEvent::Finished(ok)).unwrap();
        });
        match event_reader.recv().unwrap() {
            PublicationEvent::Finished(ok) => {
                panic!("bounded preparation must reach the final publication gate: success={ok}")
            }
            PublicationEvent::Prepared => {
                assert_eq!(
                    receiver.db.current_lsn(),
                    old_lsn,
                    "acceptance position stays unpublished until the final commit"
                );
                assert_eq!(
                    format!("{:?}", receiver.db.published_state_snapshot_for_test()),
                    old_publication,
                    "no schema, history, watermark or identity is public before the final commit"
                );
                release.send(()).unwrap();
            }
        }
        assert!(matches!(
            event_reader.recv().unwrap(),
            PublicationEvent::Finished(true)
        ));
        task.join().unwrap();
    });
    exact(&receiver.db, N, true, retained_at);
    let receiver = reopen(receiver, WRITER);
    exact(&receiver.db, N, true, retained_at);
    receiver.db.close().unwrap();
    author.db.close().unwrap();
}

#[test]
#[serial]
fn production_legacy_budget_refusal_releases_population_and_retries_at_fixed_budget() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let (author, receiver, before, retained_at) = seeded(root.path(), N);
    let delivery = Delivery::authored_by(&author, before);
    receiver.db.with_reader_memory_high_water_for_test(|| ());
    receiver.db.close().unwrap();
    drop(receiver.db);
    let path = receiver.path;
    let identity = receiver.identity;
    physical_lifecycle(|| {
        let receiver = JourneyPeer {
            db: open(&path, WRITER),
            path: path.clone(),
            identity: identity.clone(),
        };
        let no_spare = receiver.db.accountant.usage().used;
        receiver.db.set_memory_limit(Some(no_spare)).unwrap();
        // The settings write is outside the refused staging operation.
        let held = receiver.db.accountant.usage().used;
        let old_lsn = receiver.db.current_lsn();
        let (outcome, memory) = receiver
            .db
            .with_reader_memory_high_water_for_test(|| delivery.apply(&receiver));
        assert!(
            outcome.is_err(),
            "no spare writer byte can admit a new working page"
        );
        drop(outcome);
        assert_eq!(receiver.db.current_lsn(), old_lsn);
        assert_eq!(receiver.db.accountant.usage().used, held);
        assert_eq!(memory.after, memory.before);
        receiver.db.set_memory_limit(Some(WRITER)).unwrap();
        exact(&receiver.db, N, false, retained_at);
        let receiver = reopen(receiver, WRITER);
        exact(&receiver.db, N, false, retained_at);
        delivery.apply(&receiver).unwrap();
        exact(&receiver.db, N, true, retained_at);
        receiver.db.close().unwrap();
        drop(receiver);
    });
    author.db.close().unwrap();
}

/// CDB-224: a stage refused at the fixed writer budget is retried on the same
/// live handle, at the same budget, without a reopen. The refusal is one-shot:
/// a competing writer owner takes every spare byte and is released before the
/// retry, so the budget itself never changes.
#[test]
#[serial]
fn production_legacy_refused_stage_retries_live_at_the_same_budget() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let (author, receiver, before, retained_at) = seeded(root.path(), N);
    let delivery = Delivery::authored_by(&author, before);
    receiver.db.with_reader_memory_high_water_for_test(|| ());
    assert_eq!(receiver.db.accountant.usage().limit, Some(WRITER));
    let old_lsn = receiver.db.current_lsn();
    let old_publication = format!("{:?}", receiver.db.published_state_snapshot_for_test());
    let held = receiver.db.accountant.usage().used;
    let competing = WRITER - held;
    receiver.db.accountant.try_allocate(competing).unwrap();
    let (outcome, memory) = receiver
        .db
        .with_reader_memory_high_water_for_test(|| delivery.apply(&receiver));
    assert!(
        matches!(
            &outcome,
            Err(Error::MemoryBudgetExceeded { budget_limit_bytes, .. }) if *budget_limit_bytes == WRITER
        ),
        "the competitor makes the stage refuse with the typed budget error at the unchanged limit: {outcome:?}"
    );
    drop(outcome);
    receiver.db.accountant.release(competing);
    assert_eq!(
        receiver.db.accountant.usage().used,
        held,
        "the refused stage returns its writer custody before the retry"
    );
    assert_eq!(memory.after, memory.before);
    assert_eq!(receiver.db.current_lsn(), old_lsn);
    assert_eq!(
        format!("{:?}", receiver.db.published_state_snapshot_for_test()),
        old_publication
    );
    assert_eq!(receiver.db.accountant.usage().limit, Some(WRITER));
    exact(&receiver.db, N, false, retained_at);
    receiver.db.accountant.reset_high_water_for_test();
    let retried = delivery.apply(&receiver);
    assert!(
        retried.is_ok(),
        "the live retry at the same budget succeeds without a reopen: {retried:?}"
    );
    drop(retried);
    assert!(receiver.db.write_memory_high_water_for_test() <= WRITER);
    assert_eq!(receiver.db.accountant.usage().limit, Some(WRITER));
    exact(&receiver.db, N, true, retained_at);
    let receiver = reopen(receiver, WRITER);
    exact(&receiver.db, N, true, retained_at);
    receiver.db.close().unwrap();
    drop(receiver);
    author.db.close().unwrap();
}
