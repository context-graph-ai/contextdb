//! A census sample holds off a maintenance step that starts while the sample
//! is collecting, so the sample is one coherent observation.
//!
//! The census collector reads the writer's named owners and then the writer
//! accountant's total. A maintenance step that ran between those two reads
//! would make the sample count one state's owners against another state's
//! total. The atomic sample takes the maintenance and commit exclusions
//! before it collects. A maintenance step started from inside the collection,
//! at the point between the component reads and the total, must meet the
//! maintenance exclusion and stay blocked until the sample's callback has
//! returned.
//!
//! The collection does not continue until the planted step has either
//! reported that it found the maintenance exclusion held, reported that it
//! waited on the commit lock instead, or finished. No elapsed time decides
//! the outcome: with the exclusion held the step reports contention; without
//! it the step finishes first and the sample tears.
//!
//! The sample is taken before any maintenance has run on this store, so a
//! mismatch here is a torn read, not an unnamed writer byte.
use crate::census_samples_around_maintenance_name_every_writer_byte::{
    NOW, maintained_partition, unnamed,
};
use contextdb_core::Wallclock;
use contextdb_engine::database::MaintenanceReport;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Sender, channel};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

type Planted = Arc<Mutex<Option<JoinHandle<contextdb_core::Result<MaintenanceReport>>>>>;

/// What the planted maintenance step did first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FirstEvent {
    /// It found the census sample's maintenance exclusion held.
    MaintenanceExclusionHeld,
    /// It waited on the commit lock without meeting a maintenance exclusion.
    CommitLockHeld,
    /// It finished while the sample was still collecting.
    Finished,
}

fn reporter(events: &Sender<FirstEvent>, event: FirstEvent) -> Arc<dyn Fn() + Send + Sync> {
    let events = Mutex::new(events.clone());
    Arc::new(move || {
        let _ = events.lock().unwrap().send(event);
    })
}

#[test]
fn a_maintenance_step_planted_inside_census_collection_waits_until_the_sample_is_complete() {
    let _clock = Wallclock::test_clock_guard(|| NOW);
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(maintained_partition(&dir.path().join("atomic-census.db")));

    let (events, first_event) = channel::<FirstEvent>();
    let finished = Arc::new(AtomicBool::new(false));
    let planted: Planted = Arc::new(Mutex::new(None));
    let observed: Arc<Mutex<Option<FirstEvent>>> = Arc::new(Mutex::new(None));
    let contention = reporter(&events, FirstEvent::MaintenanceExclusionHeld);
    let hook = {
        let db = Arc::clone(&db);
        let finished = Arc::clone(&finished);
        let planted = Arc::clone(&planted);
        let observed = Arc::clone(&observed);
        move || {
            let step = std::thread::spawn(move || {
                let _clock = Wallclock::test_clock_guard(|| NOW);
                let report = contextdb_tx::commit_wait_test_seams::observe(
                    reporter(&events, FirstEvent::CommitLockHeld),
                    || db.run_maintenance_cycle(),
                );
                finished.store(true, Ordering::SeqCst);
                let _ = events.send(FirstEvent::Finished);
                report
            });
            *planted.lock().unwrap() = Some(step);
            // Collection continues only once the planted step has shown what
            // it met. The bound is a failsafe against a missing event, not a
            // latency claim; reaching it is reported as its own failure.
            *observed.lock().unwrap() = first_event
                .recv_timeout(std::time::Duration::from_secs(300))
                .ok();
        }
    };
    let (sample, finished_inside_sample) = db
        .with_writer_memory_ownership_contention_observer_for_test(contention, || {
            db.with_writer_memory_ownership_collection_hook_for_test(hook, || {
                db.with_atomic_writer_memory_ownership_receipt_for_test(|receipt| {
                    (receipt, finished.load(Ordering::SeqCst))
                })
            })
        });
    let step = planted
        .lock()
        .unwrap()
        .take()
        .expect("the collection hook fired and planted one maintenance step");
    let report = step.join().expect("planted maintenance step");
    let first = observed.lock().unwrap().take();
    let accountant_total = sample.vector.accountant_total;
    let named_owner_total = sample.named_owner_total;
    let reconciliation_gap = sample.reconciliation_gap;

    assert_eq!(
        first,
        Some(FirstEvent::MaintenanceExclusionHeld),
        "the maintenance step planted inside census collection must first find the \
         sample's maintenance exclusion held (CommitLockHeld: it met only the commit lock; \
         Finished: nothing held it off; None: it reported neither contention nor completion \
         within the failsafe); that sample: writer accountant total {accountant_total}, \
         named writer owners {named_owner_total}, reconciliation gap {reconciliation_gap}, \
         step finished inside the sample {finished_inside_sample}"
    );
    assert!(
        !finished_inside_sample,
        "a maintenance step planted inside census collection completed before the \
         sample's callback returned: the sample does not exclude maintenance"
    );
    if let Some(torn) = unnamed(&sample, "sample with a planted maintenance step") {
        panic!("census sample is torn across a maintenance step: {torn}");
    }
    assert!(
        report.is_ok(),
        "the held-off step completes once the sample releases: {report:?}"
    );
    let after = db.with_atomic_writer_memory_ownership_receipt_for_test(|receipt| receipt);
    assert_ne!(
        after.vector.accountant_total, sample.vector.accountant_total,
        "the planted step moved the writer accountant once it ran, so holding it off mattered"
    );
}
