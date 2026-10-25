//! A census sample holds off a maintenance step that starts while the sample
//! is collecting, so the sample is one coherent observation.
//!
//! The census collector reads the writer's named owners and then the writer
//! accountant's total. A maintenance step that ran between those two reads
//! would make the sample count one state's owners against another state's
//! total. The atomic sample takes the maintenance and commit exclusions
//! before it collects; a maintenance step started from inside the collection,
//! at the point between the component reads and the total, must stay blocked
//! until the sample's callback has returned.
//!
//! The sample is taken before any maintenance has run on this store, so a
//! mismatch here is a torn read, not an unnamed writer byte.
use crate::census_samples_around_maintenance_name_every_writer_byte::{
    NOW, maintained_partition, unnamed,
};
use contextdb_core::Wallclock;
use contextdb_engine::database::MaintenanceReport;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

type Planted = Arc<Mutex<Option<JoinHandle<contextdb_core::Result<MaintenanceReport>>>>>;

#[test]
fn a_maintenance_step_planted_inside_census_collection_waits_until_the_sample_is_complete() {
    let _clock = Wallclock::test_clock_guard(|| NOW);
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(maintained_partition(&dir.path().join("atomic-census.db")));

    let finished = Arc::new(AtomicBool::new(false));
    let planted: Planted = Arc::new(Mutex::new(None));
    let hook = {
        let db = Arc::clone(&db);
        let finished = Arc::clone(&finished);
        let planted = Arc::clone(&planted);
        move || {
            let step = std::thread::spawn(move || {
                let _clock = Wallclock::test_clock_guard(|| NOW);
                let report = db.run_maintenance_cycle();
                finished.store(true, Ordering::SeqCst);
                report
            });
            *planted.lock().unwrap() = Some(step);
        }
    };
    let (sample, finished_inside_sample) = db
        .with_writer_memory_ownership_collection_hook_for_test(hook, || {
            db.with_atomic_writer_memory_ownership_receipt_for_test(|receipt| {
                (receipt, finished.load(Ordering::SeqCst))
            })
        });
    let step = planted
        .lock()
        .unwrap()
        .take()
        .expect("the collection hook fired and planted one maintenance step");
    let report = step.join().expect("planted maintenance step");
    assert!(report.is_ok(), "the planted step completes: {report:?}");

    assert!(
        !finished_inside_sample,
        "a maintenance step planted inside census collection completed before the \
         sample's callback returned: the sample does not exclude maintenance"
    );
    if let Some(torn) = unnamed(&sample, "sample with a planted maintenance step") {
        panic!("census sample is torn across a maintenance step: {torn}");
    }
    let after = db.with_atomic_writer_memory_ownership_receipt_for_test(|receipt| receipt);
    assert_ne!(
        after.vector.accountant_total, sample.vector.accountant_total,
        "the planted step moved the writer accountant once it ran, so holding it off mattered"
    );
}
