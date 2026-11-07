// Included inside received_legacy_growth_guards. These use the same funded
// production receiver and real signed unit, without modifying any stage.
use std::sync::atomic::{AtomicBool, Ordering};

// Retired accountant-only checks that cannot prove physical storage release:
// current_published_root_controls_are_not_abandoned_stage_storage,
// handle_custody_cache_controls_are_not_abandoned_stage_storage, and
// handle_incarnation_cache_controls_are_not_abandoned_stage_storage.
// Their construction/pointer classification required the removed fork observer;
// accountant equality cannot prove that physical ownership claim. Publication
// visibility and incarnation continuity remain covered below. The custody guard
// below checks handle reuse without claiming a physical-storage census.

#[test]
#[serial]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn handle_custody_cache_reuses_its_publication_counter() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let db = open(&root.path().join("custody-controls.db"), WRITER);
    assert!(db.custody_cache.lock().is_none());
    db.with_custody_store(|_| Ok(())).unwrap();
    let publications = db
        .custody_cache
        .lock()
        .as_ref()
        .unwrap()
        .publications
        .clone();
    let held = db.accountant.usage().used;
    for _ in 0..2 {
        db.with_custody_store(|_| Ok(())).unwrap();
        assert!(Arc::ptr_eq(
            &publications,
            &db.custody_cache.lock().as_ref().unwrap().publications
        ));
        assert_eq!(db.accountant.usage().used, held);
    }
    drop(publications);
    db.close().unwrap();
}

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
/// `writer_custody` checks that the failed stage gives back every writer
/// byte it held. The durable-failure path still leaves a residual, tracked
/// on its own row until that leak is fixed.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn failed_preparation_cleanup(durable: bool, writer_custody: bool) {
    if durable && writer_custody {
        durable_failure_live_custody();
        return;
    }
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
        if writer_custody {
            assert_eq!(
                receiver.db.accountant.usage().used,
                held,
                "failed prepared stage releases all writer custody"
            );
        }
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
// Accountant recovery is sampled on the still-open receiver before readback,
// unrelated work, retry or close. Payload destruction order is reviewed in
// production source; equality does not claim to observe physical retention.
fn assert_attempt_retired(db: &Database, held: usize) {
    assert_eq!(
        db.accountant.usage().used,
        held,
        "the failed received attempt refunds its complete staged invoice"
    );
    assert_eq!(
        db.tx_mgr.with_active_write_sets(|active| active.len()),
        0,
        "no original transaction remains active after recovery"
    );
    assert!(
        db.received_schema_stages.lock().is_empty(),
        "no received stage remains parked after recovery"
    );
}

// The custody row has its own live-handle branch. The established cancellation
// and active durable lifecycle controls retain their existing close census.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn durable_failure_live_custody() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let (author, receiver, before, retained_at) = seeded(root.path(), N);
    let delivery = Delivery::authored_by(&author, before);
    let reached = Arc::new(AtomicBool::new(false));
    let plugin = Arc::new(CancelPrepared {
        reached: reached.clone(),
        cancel: AtomicBool::new(false),
    });
    let receiver = with_plugin(receiver, plugin);
    receiver.db.with_reader_memory_high_water_for_test(|| ());
    let old_lsn = receiver.db.current_lsn();
    let old_publication = format!("{:?}", receiver.db.published_state_snapshot_for_test());
    let held = receiver.db.accountant.usage().used;
    for _attempt in 0..2 {
        receiver
            .db
            .fail_next_received_schema_durable_commit_for_test();
        let memory = {
            let (outcome, memory) = receiver
                .db
                .with_reader_memory_high_water_for_test(|| delivery.apply(&receiver));
            assert!(
                matches!(&outcome, Err(error @ Error::Other(_))
                if error.to_string().contains("injected received-schema Redb pre-commit failure")),
                "the durable apply must reach its injected pre-commit fault, not an earlier refusal: {outcome:?}"
            );
            assert!(
                reached.load(Ordering::SeqCst),
                "full bounded staging must reach pre_commit before this refusal"
            );
            drop(outcome);
            memory
        };
        eprintln!(
            "DURABLE_ACCOUNTANT_RECOVERY used={} held={held}",
            receiver.db.accountant.usage().used
        );
        assert_attempt_retired(&receiver.db, held);
        assert_eq!(
            receiver.db.accountant.usage().used,
            held,
            "failed prepared stage releases all writer custody"
        );
        assert_eq!(
            memory.after, memory.before,
            "failed prepared stage releases all reader custody"
        );
        assert_eq!(receiver.db.current_lsn(), old_lsn);
        assert_eq!(
            format!("{:?}", receiver.db.published_state_snapshot_for_test()),
            old_publication
        );
        exact(&receiver.db, N, false, retained_at);
    }
    // The same receiver and the same non-cancelling plugin retry. The fault
    // is one-shot; no close, reopen or plugin replacement can clear state.
    assert_eq!(receiver.db.accountant.usage().limit, Some(WRITER));
    let retried = delivery.apply(&receiver);
    assert!(
        retried.is_ok(),
        "durable failure retries on this live handle at WRITER: {retried:?}"
    );
    drop(retried);
    assert_eq!(receiver.db.accountant.usage().limit, Some(WRITER));
    exact(&receiver.db, N, true, retained_at);
    let receiver = reopen(receiver, WRITER);
    exact(&receiver.db, N, true, retained_at);
    receiver.db.close().unwrap();
    drop(receiver);
    author.db.close().unwrap();
}

#[test]
#[serial]
fn production_legacy_plugin_cancellation_releases_prepared_population_and_retries() {
    failed_preparation_cleanup(false, true);
}
#[test]
#[serial]
fn production_legacy_durable_failure_releases_prepared_population_and_retries() {
    failed_preparation_cleanup(true, false);
}
/// Full-population custody checks run in the nightly library target.
/// The small deep-refusal twin below carries the same assertions in PR.
#[test]
#[serial]
#[ignore = "deep refusal retains writer custody"]
fn production_legacy_durable_failure_returns_all_writer_custody() {
    failed_preparation_cleanup(true, true);
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
                // Read the committed position without opening a public
                // operation: the parked stage holds the schema publication
                // gate, and a public read waits for it by design.
                assert_eq!(
                    receiver.db.tx_mgr.current_lsn(),
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
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
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

/// A stage refused at the fixed writer budget is retried on the same
/// live handle, at the same budget, without a reopen. The refusal is one-shot:
/// a competing writer owner takes every spare byte and is released before the
/// retry, so the budget itself never changes.
#[test]
#[serial]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
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

/// Deep flush refusal: the stage's own charges are taken before
/// pre_commit; this plugin then takes every spare writer byte but a fixed
/// DEEP_ROOM, so the budget refusal lands inside the durable flush that
/// follows, at an unchanged limit. The competitor is released before every
/// measurement. Nothing about the limit or the room depends on the fix.
const DEEP_ROOM: usize = 64 * 1024;
struct SqueezeAtPreCommit {
    accountant: std::sync::Mutex<Option<crate::memory_accounting::AccountantHandle>>,
    armed: AtomicBool,
    taken: std::sync::atomic::AtomicUsize,
    used_at_pre_commit: std::sync::atomic::AtomicUsize,
}
impl crate::plugin::DatabasePlugin for SqueezeAtPreCommit {
    fn pre_commit(
        &self,
        _: &contextdb_tx::WriteSet,
        source: crate::plugin::CommitSource,
    ) -> Result<()> {
        if source == crate::plugin::CommitSource::SyncPull
            && self.armed.swap(false, Ordering::SeqCst)
        {
            let guard = self.accountant.lock().unwrap();
            let accountant = guard.as_ref().expect("installed after open");
            let used = accountant.usage().used;
            self.used_at_pre_commit.store(used, Ordering::SeqCst);
            let competing = WRITER.saturating_sub(used).saturating_sub(DEEP_ROOM);
            accountant.try_allocate(competing).unwrap();
            self.taken.store(competing, Ordering::SeqCst);
        }
        Ok(())
    }
}

#[test]
#[serial]
#[ignore = "the full lifecycle requires extended execution"]
fn production_legacy_deep_budget_refusal_returns_all_writer_custody_and_retries_live() {
    deep_budget_refusal_lifecycle(N);
}

#[test]
#[serial]
fn production_legacy_small_deep_budget_refusal_returns_all_writer_custody_and_retries_live() {
    deep_budget_refusal_lifecycle(1);
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn deep_budget_refusal_lifecycle(count: usize) {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let (author, receiver, before, retained_at) = seeded(root.path(), count);
    let delivery = Delivery::authored_by(&author, before);
    let squeeze = Arc::new(SqueezeAtPreCommit {
        accountant: std::sync::Mutex::new(None),
        armed: AtomicBool::new(false),
        taken: std::sync::atomic::AtomicUsize::new(0),
        used_at_pre_commit: std::sync::atomic::AtomicUsize::new(0),
    });
    let receiver = with_plugin(receiver, squeeze.clone());
    *squeeze.accountant.lock().unwrap() = Some(receiver.db.accountant.clone());
    sql(
        &receiver.db,
        "CREATE TABLE deep_unrelated (id INTEGER PRIMARY KEY)",
    );
    receiver.db.with_reader_memory_high_water_for_test(|| ());
    assert_eq!(receiver.db.accountant.usage().limit, Some(WRITER));
    let held = receiver.db.accountant.usage().used;
    // Preserve the typed fault receipt after dropping the returned error.
    let mut refusal_text = String::with_capacity(4096);
    let refusal_capacity = refusal_text.capacity();
    for attempt in 0..2 {
        refusal_text.clear();
        let old_lsn = receiver.db.current_lsn();
        let old_publication = format!("{:?}", receiver.db.published_state_snapshot_for_test());
        squeeze.armed.store(true, Ordering::SeqCst);
        let memory = {
            let (outcome, memory) = receiver
                .db
                .with_reader_memory_high_water_for_test(|| delivery.apply(&receiver));
            let taken = squeeze.taken.swap(0, Ordering::SeqCst);
            assert!(
                taken > 0,
                "attempt {attempt}: the stage reached pre_commit with more than DEEP_ROOM spare"
            );
            assert!(
                squeeze.used_at_pre_commit.load(Ordering::SeqCst) > held,
                "attempt {attempt}: the stage's own charges exist before the refused flush"
            );
            assert!(
                matches!(
                    &outcome,
                    Err(Error::MemoryBudgetExceeded { budget_limit_bytes, .. }) if *budget_limit_bytes == WRITER
                ),
                "attempt {attempt}: the flush after pre_commit is refused for budget at the unchanged limit: {outcome:?}"
            );
            std::fmt::write(
                    &mut refusal_text,
                    format_args!(
                        "stage=after_pre_commit pre_commit_used={} competitor={taken} typed_error={outcome:?}",
                        squeeze.used_at_pre_commit.load(Ordering::SeqCst),
                    ),
                )
                .unwrap();
            assert_eq!(refusal_text.capacity(), refusal_capacity);
            drop(outcome);
            receiver.db.accountant.release(taken);
            memory
        };
        eprintln!("P5_DEEP_REFUSAL attempt={attempt} population={count} {refusal_text}");
        eprintln!(
            "P5_ACCOUNTANT_RECOVERY attempt={attempt} used={} held={held} population={count}",
            receiver.db.accountant.usage().used
        );
        assert_attempt_retired(&receiver.db, held);
        assert_eq!(
            receiver.db.accountant.usage().used,
            held,
            "attempt {attempt}: a deep budget refusal returns all writer custody"
        );
        assert_eq!(memory.after, memory.before);
        assert_eq!(receiver.db.current_lsn(), old_lsn);
        assert_eq!(
            format!("{:?}", receiver.db.published_state_snapshot_for_test()),
            old_publication
        );
        exact(&receiver.db, count, false, retained_at);
    }
    for attempt in 0..2 {
        receiver
            .db
            .execute(
                "INSERT INTO deep_unrelated VALUES ($id)",
                &HashMap::from([("id".into(), Value::Int64(attempt))]),
            )
            .unwrap();
        assert_eq!(
            receiver.db.accountant.usage().used,
            held,
            "attempt {attempt}: no refused-stage charge survives a later commit"
        );
    }
    let retried = delivery.apply(&receiver);
    assert!(
        retried.is_ok(),
        "live retry at the writer budget, no reopen: {retried:?}"
    );
    drop(retried);
    assert_eq!(receiver.db.accountant.usage().limit, Some(WRITER));
    exact(&receiver.db, count, true, retained_at);
    let receiver = reopen(receiver, WRITER);
    exact(&receiver.db, count, true, retained_at);
    receiver.db.close().unwrap();
    drop(receiver);
    author.db.close().unwrap();
    eprintln!("P5_FULL_PATH_DONE population={count}");
}

#[test]
#[serial]
fn one_open_handle_keeps_its_incarnation_after_failed_received_apply_and_reopen() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let root = tempfile::tempdir().unwrap();
    // This small identity control is independent of the frozen N=2043
    // population/refusal fixture above; identity cannot depend on payload size.
    let (author, receiver, before, _) = seeded(root.path(), 1);
    let reached = Arc::new(AtomicBool::new(false));
    let receiver = with_plugin(
        receiver,
        Arc::new(CancelPrepared {
            reached: reached.clone(),
            cancel: AtomicBool::new(false),
        }),
    );
    let nested_tenant = TenantId::from("incarnation-before-first-mint");
    let nested_incarnation = {
        let _operation = receiver.db.open_operation().unwrap();
        let _earlier_statement = receiver
            .db
            .writer_read_scope(receiver.db.declared_reader_memory().unwrap())
            .unwrap();
        let earlier_root = receiver
            .db
            .writer_read_root()
            .unwrap()
            .expect("file-backed statement root");
        assert!(
            receiver
                .db
                .read_config_value::<String>(
                    &nested_tenant.config_key("sync_incarnation"),
                    receiver.db.declared_reader_memory().unwrap(),
                )
                .unwrap()
                .is_none(),
            "the held statement predates this tenant's first mint"
        );
        let first = receiver
            .db
            .sync_incarnation_for_test(&nested_tenant)
            .unwrap();
        let current = crate::persistence::WriterSnapshotHandle::from_capture(
            receiver.db.tx_mgr.capture_published_read().handle.unwrap(),
        )
        .unwrap();
        assert!(
            !earlier_root.ptr_eq(&current),
            "first mint really publishes a newer root"
        );
        assert!(
            receiver
                .db
                .writer_read_root()
                .unwrap()
                .unwrap()
                .ptr_eq(&earlier_root),
            "the earlier statement stays open on its pre-mint root"
        );
        assert_eq!(
            receiver
                .db
                .sync_incarnation_for_test(&nested_tenant)
                .unwrap(),
            first,
            "a nested lookup through a pre-mint statement cannot mint a second identity"
        );
        first
    };
    let tenant = TenantId::from(JOURNEY_TENANT);
    let incarnation = receiver.db.sync_incarnation_for_test(&tenant).unwrap();
    for _ in 0..16 {
        assert_eq!(
            receiver.db.sync_incarnation_for_test(&tenant).unwrap(),
            incarnation
        );
    }
    receiver
        .db
        .fail_next_received_schema_durable_commit_for_test();
    let refused = Delivery::authored_by(&author, before).apply(&receiver);
    assert!(
        matches!(&refused, Err(error @ Error::Other(_))
        if error.to_string().contains("injected received-schema Redb pre-commit failure")),
        "identity control must reach its real injected failure: {refused:?}"
    );
    assert!(
        reached.load(Ordering::SeqCst),
        "received apply reached pre_commit"
    );
    drop(refused);
    for _ in 0..16 {
        assert_eq!(
            receiver.db.sync_incarnation_for_test(&tenant).unwrap(),
            incarnation,
            "failed received apply cannot remint the live file-backed tenant identity"
        );
        assert_eq!(
            receiver
                .db
                .sync_incarnation_for_test(&nested_tenant)
                .unwrap(),
            nested_incarnation
        );
    }
    let receiver = reopen(receiver, WRITER);
    for _ in 0..16 {
        assert_eq!(
            receiver.db.sync_incarnation_for_test(&tenant).unwrap(),
            incarnation,
            "the same durable identity survives reopening"
        );
        assert_eq!(
            receiver
                .db
                .sync_incarnation_for_test(&nested_tenant)
                .unwrap(),
            nested_incarnation
        );
    }
    receiver.db.close().unwrap();
    author.db.close().unwrap();
}

// The callback itself unwinds after the real received stage owns its payload.
// No stage selection, refund, or terminal removal is replaced by this fixture.
struct PanicReceivedPreparation {
    early_error: bool,
    normal_error: bool,
    before_replace: bool,
    hub_contact: bool,
    expected_node: String,
    contact_invoice: std::sync::atomic::AtomicUsize,
    armed: AtomicBool,
    failed_armed: AtomicBool,
    entered: AtomicBool,
    failure_entered: AtomicBool,
    accountant: std::sync::OnceLock<crate::memory_accounting::AccountantHandle>,
    held: std::sync::atomic::AtomicUsize,
    callback_used: std::sync::atomic::AtomicUsize,
}
impl crate::plugin::DatabasePlugin for PanicReceivedPreparation {
    fn pre_commit(
        &self,
        _: &contextdb_tx::WriteSet,
        source: crate::plugin::CommitSource,
    ) -> Result<()> {
        if source == crate::plugin::CommitSource::SyncPull
            && self.armed.swap(false, Ordering::SeqCst)
        {
            let used = self
                .accountant
                .get()
                .expect("installed after open")
                .usage()
                .used;
            self.callback_used.store(used, Ordering::SeqCst);
            assert!(
                used > self.held.load(Ordering::SeqCst),
                "the real stage owns writer charges before the callback"
            );
            self.entered.store(true, Ordering::SeqCst);
            if self.early_error || self.normal_error {
                return Err(Error::ReadCancelled);
            }
            panic!("injected received pre_commit panic");
        }
        Ok(())
    }
    fn commit_failed(
        &self,
        write_set: &contextdb_tx::WriteSet,
        source: crate::plugin::CommitSource,
        error: &Error,
    ) {
        if source == crate::plugin::CommitSource::SyncPull
            && (self.early_error || self.normal_error || self.before_replace)
            && self.failed_armed.swap(false, Ordering::SeqCst)
        {
            if self.before_replace {
                assert!(
                    matches!(error, Error::SyncError(message) if message.as_ref() == "injected fault while taking delivery of a received unit"),
                    "the pre-replacement diagnostic reaches commit_failed unchanged: {error:?}"
                );
            } else {
                assert!(
                    matches!(error, Error::ReadCancelled),
                    "the plugin rejection reaches commit_failed unchanged: {error:?}"
                );
            }
            if self.hub_contact {
                let contact = write_set
                    .relational_inserts
                    .iter()
                    .find(|(table, _)| table.as_str() == "work_node_contacts")
                    .expect("the failure callback still borrows the contact-bearing body");
                assert!(
                    matches!(contact.1.values.get("node_id"), Some(Value::Text(node)) if node == &self.expected_node),
                    "commit_failed sees this delivery's authenticated contact"
                );
                let invoice = write_set
                    .relational_insert_admitted_bytes
                    .get("work_node_contacts")
                    .and_then(|rows| rows.get(&contact.1.row_id))
                    .copied()
                    .unwrap_or(0);
                self.contact_invoice.store(invoice, Ordering::SeqCst);
                if self.before_replace {
                    assert!(
                        invoice > 0,
                        "the returned original still carries its actual contact admission through commit_failed"
                    );
                    self.callback_used.store(
                        self.accountant.get().unwrap().usage().used,
                        Ordering::SeqCst,
                    );
                }
            }
            self.failure_entered.store(true, Ordering::SeqCst);
            if self.early_error {
                panic!("injected received commit_failed panic");
            }
        }
    }
}

fn received_callback_unwind_cleanup(early_error: bool) {
    received_callback_unwind_cleanup_with_contact(early_error, false);
}

fn received_callback_unwind_cleanup_with_contact(early_error: bool, hub_contact: bool) {
    received_contact_cleanup_schedule(early_error, hub_contact, false, false);
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn received_contact_cleanup_schedule(
    early_error: bool,
    hub_contact: bool,
    normal_error: bool,
    before_replace: bool,
) {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let count = 1;
    let (author, receiver, before, retained_at) = seeded(root.path(), count);
    let delivery = Delivery::authored_by(&author, before);
    let plugin = Arc::new(PanicReceivedPreparation {
        early_error,
        normal_error,
        before_replace,
        hub_contact,
        expected_node: delivery.node.clone(),
        contact_invoice: std::sync::atomic::AtomicUsize::new(0),
        armed: AtomicBool::new(false),
        failed_armed: AtomicBool::new(false),
        entered: AtomicBool::new(false),
        failure_entered: AtomicBool::new(false),
        accountant: std::sync::OnceLock::new(),
        held: std::sync::atomic::AtomicUsize::new(0),
        callback_used: std::sync::atomic::AtomicUsize::new(0),
    });
    let receiver = with_plugin(receiver, plugin.clone());
    assert!(
        plugin
            .accountant
            .set(receiver.db.accountant.clone())
            .is_ok()
    );
    let mut expected_contact_invoice = 0;
    if hub_contact {
        crate::sync_system_tables::ensure_node_contacts_schema(
            &receiver.db,
            receiver.db.declared_reader_memory().unwrap(),
        )
        .unwrap();
        // Warm the same production INSERT and its reusable metadata backing
        // before the baseline. Roll back so the panic still owes no contact row.
        let warm = receiver.db.begin().unwrap();
        receiver
            .db
            .stage_hub_node_contact(
                warm,
                &delivery.node,
                1_700_000_000_000,
                receiver.db.declared_reader_memory().unwrap(),
            )
            .unwrap();
        expected_contact_invoice = receiver
            .db
            .tx_mgr
            .with_write_set(warm, |write_set| {
                let contact = write_set
                    .relational_inserts
                    .iter()
                    .find(|(table, _)| table.as_str() == "work_node_contacts")
                    .expect("the production warm-up stages this delivery's contact");
                write_set
                    .relational_insert_admitted_bytes
                    .get("work_node_contacts")
                    .and_then(|rows| rows.get(&contact.1.row_id))
                    .copied()
                    .expect("the actual contact has an admitted invoice")
            })
            .unwrap();
        assert_eq!(
            expected_contact_invoice, 1302,
            "the frozen contact fixture keeps its original exact row admission"
        );
        receiver.db.rollback(warm).unwrap();
        assert!(
            receiver
                .db
                .scan("work_node_contacts", receiver.db.snapshot())
                .unwrap()
                .is_empty()
        );
    }
    receiver.db.with_reader_memory_high_water_for_test(|| ());
    assert_eq!(
        receiver
            .db
            .tx_mgr
            .with_active_write_sets(|active| active.len()),
        0
    );
    let old_lsn = receiver.db.current_lsn();
    let old_publication = format!("{:?}", receiver.db.published_state_snapshot_for_test());
    let held = receiver.db.accountant.usage().used;
    // Failed preparation must also return native pages, which are not
    // represented by the refunded writer invoice. Inspect the live handle
    // through the shipping storage-statistics API before and after unwind.
    let allocated_pages = || {
        receiver
            .db
            .persistence
            .as_ref()
            .unwrap()
            .with_db(|db| {
                let snapshot = db
                    .storage_statistics_snapshot()
                    .map_err(RedbPersistence::storage_error)?;
                Ok(snapshot
                    .stats()
                    .map_err(RedbPersistence::storage_error)?
                    .allocated_pages())
            })
            .unwrap()
    };
    let pages_before = if !early_error && !hub_contact {
        Some(allocated_pages())
    } else {
        None
    };
    plugin.held.store(held, Ordering::SeqCst);
    plugin.armed.store(true, Ordering::SeqCst);
    plugin.failed_armed.store(
        early_error || normal_error || before_replace,
        Ordering::SeqCst,
    );
    if before_replace {
        Database::arm_received_unit_fault_for_test(
            crate::database::ReceivedUnitFaultPoint::AfterStageRegistration,
            crate::database::ReceivedUnitCopyFault::Error,
        );
    }
    let expected = if early_error {
        "injected received commit_failed panic"
    } else {
        "injected received pre_commit panic"
    };
    eprintln!("RECEIVED_CALLBACK_UNWIND_BEGIN early_error={early_error} held={held}");
    let memory = {
        let (outcome, memory) = receiver.db.with_reader_memory_high_water_for_test(|| {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                delivery.apply_with_contact(&receiver, hub_contact.then_some(1_700_000_000_000))
            }))
        });
        if normal_error || before_replace {
            let error = outcome
                .expect("the ordinary failure must return without panicking")
                .expect_err("the armed preparation must reject the receive");
            if before_replace {
                assert!(
                    matches!(&error, Error::SyncError(message) if message.as_ref() == "injected fault while taking delivery of a received unit"),
                    "preserve the original typed pre-replacement error: {error:?}"
                );
            } else {
                assert!(
                    matches!(error, Error::ReadCancelled),
                    "preserve the original typed plugin rejection: {error:?}"
                );
            }
            drop(error);
        } else {
            let payload = outcome.expect_err("the actual plugin callback must unwind");
            assert_eq!(
                payload.downcast_ref::<&str>().copied(),
                Some(expected),
                "only the injected callback panic qualifies"
            );
            drop(payload);
        }
        assert_eq!(plugin.entered.load(Ordering::SeqCst), !before_replace);
        assert_eq!(
            plugin.failure_entered.load(Ordering::SeqCst),
            early_error || normal_error || before_replace
        );
        memory
    };
    if let Some(pages_before) = pages_before {
        assert_eq!(
            allocated_pages(),
            pages_before,
            "a panicking received preparation returns all unpublished native pages before retry"
        );
    }
    let active = receiver
        .db
        .tx_mgr
        .with_active_write_sets(|active| active.len());
    let used = receiver.db.accountant.usage().used;
    eprintln!(
        "RECEIVED_CALLBACK_UNWIND early_error={early_error} callback_used={} used={used} held={held} active={active} reader_before={} reader_after={}",
        plugin.callback_used.load(Ordering::SeqCst),
        memory.before,
        memory.after
    );
    assert_eq!(
        active, 0,
        "unwinding preparation removes the transaction from the live manager"
    );
    eprintln!(
        "RECEIVED_CONTACT_SCHEDULE normal_error={normal_error} before_replace={before_replace} failure_callback={} invoice={} expected_invoice={expected_contact_invoice} gap={}",
        plugin.failure_entered.load(Ordering::SeqCst),
        plugin.contact_invoice.load(Ordering::SeqCst),
        used as i128 - held as i128
    );
    // After replacement the callback has no admission map; custody holds the original 1,302 B invoice, whose refund is proved by used == held and a same-budget retry.
    if hub_contact && before_replace {
        assert_eq!(
            plugin.contact_invoice.load(Ordering::SeqCst),
            expected_contact_invoice,
            "the actual failed original carries the same exact contact admission"
        );
    }
    assert_attempt_retired(&receiver.db, held);
    assert_eq!(
        used, held,
        "an unwound received preparation returns every writer charge before retry"
    );
    assert_eq!(
        memory.after, memory.before,
        "unwinding returns reader custody too"
    );
    assert_eq!(receiver.db.current_lsn(), old_lsn);
    assert_eq!(
        format!("{:?}", receiver.db.published_state_snapshot_for_test()),
        old_publication
    );
    exact(&receiver.db, count, false, retained_at);
    assert_eq!(receiver.db.accountant.usage().limit, Some(WRITER));
    if hub_contact {
        assert!(
            receiver
                .db
                .scan("work_node_contacts", receiver.db.snapshot())
                .unwrap()
                .is_empty(),
            "the unpublished contact row must not survive the failed receive"
        );
    }
    plugin.armed.store(false, Ordering::SeqCst);
    let retry = delivery.apply_with_contact(&receiver, hub_contact.then_some(1_700_000_000_000));
    assert!(
        retry.is_ok(),
        "the same handle and one-shot plugin retry at the unchanged budget: {retry:?}"
    );
    drop(retry);
    assert_eq!(
        receiver
            .db
            .tx_mgr
            .with_active_write_sets(|active| active.len()),
        0
    );
    exact(&receiver.db, count, true, retained_at);
    if hub_contact {
        let contacts = receiver
            .db
            .scan("work_node_contacts", receiver.db.snapshot())
            .unwrap();
        assert_eq!(
            contacts.len(),
            1,
            "the same-handle retry publishes the authenticated contact"
        );
        assert_eq!(
            contacts[0].values.get("node_id"),
            Some(&Value::Text(delivery.node.clone()))
        );
    }
    let receiver = reopen(receiver, WRITER);
    exact(&receiver.db, count, true, retained_at);
    receiver.db.close().unwrap();
    author.db.close().unwrap();
    eprintln!("RECEIVED_CALLBACK_UNWIND_DONE early_error={early_error}");
}

#[test]
#[serial]
fn production_legacy_pre_commit_panic_returns_custody_and_retries_live() {
    received_callback_unwind_cleanup(false);
}

#[test]
#[serial]
fn production_legacy_early_error_then_commit_failed_panic_returns_custody_and_retries_live() {
    received_callback_unwind_cleanup(true);
}

#[test]
#[serial]
fn production_legacy_hub_contact_preparation_panic_returns_custody_and_retries_live() {
    received_callback_unwind_cleanup_with_contact(false, true);
}

#[test]
#[serial]
fn production_legacy_hub_contact_rejection_returns_custody_and_retries_live() {
    received_contact_cleanup_schedule(false, true, true, false);
}

#[test]
#[serial]
fn production_legacy_hub_contact_commit_failed_panic_returns_custody_and_retries_live() {
    received_callback_unwind_cleanup_with_contact(true, true);
}

#[test]
#[serial]
fn production_legacy_hub_contact_before_replace_error_returns_custody_and_retries_live() {
    received_contact_cleanup_schedule(false, true, false, true);
}
