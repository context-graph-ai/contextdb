//! A selected membership keeps the one owning-row proof it already paid for.
use super::*;
use crate::persistence::vector_membership_source::{DurableMembershipSource, MembershipOwner};
use contextdb_vector::store::{RawVectorMembershipSource, DormantRawVectorLoader};

#[test]
#[serial_test::serial]
fn selected_historical_membership_and_body_reconcile_the_owning_row_once() {
    let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_700_000_000_000);
    for rows in [32, 64] {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open_with_options(
            dir.path().join("selected-proof.redb"),
            DatabaseOpenOptions {
                maintenance_policy: MaintenancePolicy::CallerDriven,
                ..Default::default()
            },
        )
        .unwrap();
        db.execute(
            "CREATE TABLE selected_proof (id INTEGER PRIMARY KEY, v VECTOR(4)) HISTORY ALL",
            &HashMap::new(),
        )
        .unwrap();
        for id in 0..rows {
            db.execute(
                "INSERT INTO selected_proof VALUES ($id,$v)",
                &HashMap::from([
                    ("id".into(), Value::Int64(id)),
                    ("v".into(), Value::Vector(vec![1.0, id as f32, 0.0, 0.0])),
                ]),
            )
            .unwrap();
        }
        let snapshot = db.snapshot();
        let child = db.ordinary_read_child().unwrap();
        crate::executor::bounded::with_native_read_memory(child, |memory| {
            let image = crate::read_image_memory::ImageMemory::new_direct_reader(None, memory.clone())?;
            image.scope(|| {
            let _intent = db.vector_historical_read_intent(snapshot, memory.clone());
            let mut copies = crate::vector_read_memory::admission(memory.clone());
            let _scope = db.vector_read_scope(snapshot, memory.clone(), &mut copies).unwrap();
            let persistence = db.persistence.as_ref().unwrap().clone();
            let partitions = persistence.durable_membership_partitions(&mut copies).unwrap();
            assert_eq!(partitions.len(), 1);
            let (partition, summary) = partitions.into_iter().next().unwrap();
            let mut workspace = copies.scratch();
            let retained = copies.scratch();
            let source = DurableMembershipSource::from_owner_admitted(
                MembershipOwner::Writable(persistence.clone()), None, &partition, summary,
                db.accountant.clone(), &mut workspace, retained,
            ).unwrap();
            // Discover the genuine physical row id through its existing
            // admitted cursor, outside the measured select/load segment.
            let mut first_window = copies.scratch();
            let mut first = source.open(&mut |request| match request {
                DormantVectorLoadRequest::Reserve(bytes) => { first_window.reserve(bytes).unwrap(); true }
                _ => true,
            }).unwrap();
            let identity = first.next_with_body_identity(&mut |request| match request {
                DormantVectorLoadRequest::Reserve(bytes) => { first_window.reserve(bytes).unwrap(); true }
                _ => true,
            }).unwrap().unwrap().0;
            drop(first);
            drop(first_window);
            let loader = DurableRawVectorPartitionLoader {
                source: DurableRawVectorSource::Writable(persistence), accountant: db.accountant.clone(),
                partition, dimension: 4, quantization: VectorQuantization::F32, metadata_copy_owner: None,
            };
            let observed = crate::read_work_probe::ReadWorkObservationScope::enter(crate::read_work_probe::ReadWorkPhase::Query);
            let selected = source.select_visible_by_row(identity.row_id, snapshot, &mut || true, &mut copies).unwrap().unwrap();
            let mut body_window = copies.scratch();
            let body_admissions = std::cell::Cell::new(0usize);
            let loaded = loader.load_selected_candidate(selected.as_ref(), true, &mut |request| match request {
                DormantVectorLoadRequest::Reserve(bytes) => { body_admissions.set(body_admissions.get() + 1); body_window.reserve(bytes).unwrap(); true }
                _ => true,
            }).unwrap();
            assert_eq!(selected.identity(), identity, "selected record preserves the genuine cursor identity; the actual loader checks its body");
            let work = observed.counters();
            let history = observed.historical_counters();
            assert_eq!(history.owning_row_controls_admitted, 1,
                "one consumed membership interval needs one successfully admitted owning-row control: {work:?}, {history:?}");
            assert_eq!(work.decoded_rows, 1, "selection/body reuse the same authenticated owning row");
            assert_eq!(body_admissions.get(), 1, "one actual selected-body reservation");
            assert_eq!(work.vector_body_accesses, 1);
            assert_eq!(work.decoded_vectors, 1);
            assert!(history.manifest_members_visited <= 2, "one version and at most one boundary witness: {history:?}");
            drop(loaded);
            drop(body_window);
            drop(selected);
            Ok(())
            })
        }).unwrap();
        db.close().unwrap();
    }
}
