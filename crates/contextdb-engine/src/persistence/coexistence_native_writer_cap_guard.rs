//! Independent native-source-cap component of the cache coexistence guard.
//! Intended child of persistence::native_storage_admission; source only, NOT RUN.
//! Uses the real authenticated writer and its actual StorageAdmissionScope.
use super::*;
use crate::memory_accounting::{MemoryAccountant, OwnedMemoryReservation};
use serial_test::serial;

const LIMIT: usize = 8 * 1024 * 1024;
const CACHE: usize = 128 * 1024;

#[test]
#[serial]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn an_original_native_writer_refusal_cannot_borrow_unused_clean_cache_room() {
    let writer = MemoryAccountant::writer_budget(LIMIT).expect("GUARD_BLOCKED: writer fixture");
    let owner = writer.accountant();
    let initial_baseline = owner.usage().used;
    let funder = writer
        .prepare_source_funder()
        .expect("GUARD_BLOCKED: genuine writer source");
    let original = writer
        .reserve_assembly_original(&funder)
        .expect("GUARD_BLOCKED: Original");
    let scope = StorageAdmissionScope::writer_original_native(&original)
        .expect("GUARD_BLOCKED: actual native writer scope");
    let mut builder = redb::Database::builder();
    builder.set_cache_size(CACHE);
    let raw = scope
        .run_native(|| {
            builder
                .create_with_backend(redb::backends::InMemoryBackend::new())
                .map_err(|error| contextdb_core::Error::Other(error.to_string().into()))
        })
        .expect("GUARD_BLOCKED: native fixture bootstrap");
    let fixed_bytes = CACHE.checked_add(raw.cache_index_bytes()).unwrap();
    let reserve = crate::persistence::cache_reserve::original(&owner, fixed_bytes, false)
        .expect("GUARD_BLOCKED: authentic fixed reserve");
    let controls = raw.retain_cache_reserve(reserve);
    let warm = raw
        .cached_header_for_test()
        .expect("GUARD_BLOCKED: warm actual header");
    let warm_identity = warm.allocation();
    let before = controls.observation_for_test();
    assert!(
        before[0].1 + before[1].1 + warm_identity.1 < CACHE,
        "GUARD_BLOCKED: this case requires unused real fixed-cache room"
    );

    // Prepay the real scope before filling the SAME original writer's declared cap.
    // No limit/default is raised or changed during the operation.
    let used = owner.usage().used;
    let remainder = LIMIT
        .checked_sub(used)
        .expect("GUARD_BLOCKED: fixture exceeded declared cap");
    let tail = OwnedMemoryReservation::try_new_for_native(
        owner.clone(),
        remainder,
        "test",
        "native source cap",
        "controlled fixture reservation",
    )
    .expect("GUARD_BLOCKED: original cap reservation");
    assert_eq!(owner.usage().used, LIMIT);
    let reference = OwnedMemoryReservation::try_new_for_native(
        owner.clone(),
        1,
        "test",
        "reference cap refusal",
        "same original writer",
    );
    assert!(
        matches!(
            reference,
            Err(NativeRefusal::MemoryBudgetExceeded {
                available_bytes: 0,
                ..
            })
        ),
        "GUARD_BLOCKED: actual original cap must positively refuse"
    );
    drop(reference);

    // Same real callback path used by the writer. Header read -> COW write exercises
    // dirty construction with the native admission installed and legacy TLS absent.
    let result = scope.run_native(|| {
        raw.lifetime_aliases_for_test()
            .map_err(crate::persistence::RedbPersistence::storage_error)
    });
    let positive_bad_success = result
        .as_ref()
        .ok()
        .map(|(_, dirty, _)| (dirty.allocation(), dirty.payment(), owner.usage().used));
    let expected_refusal = matches!(
        &result,
        Err(NativeStorageFailure::Refused(
            NativeRefusal::MemoryBudgetExceeded {
                available_bytes: 0,
                ..
            }
        ))
    );
    let unexpected_failure = result.is_err() && !expected_refusal;
    // No assertion can strand fixture ownership. Drop every returned alias/control,
    // then release reservation, cache and scope before reporting the invariant.
    drop(result);
    drop(tail);
    drop(warm);
    drop(controls);
    drop(raw);
    drop(scope);
    drop(original);
    drop(funder);
    let settled = owner.usage().used;
    assert!(
        !unexpected_failure,
        "GUARD_BLOCKED: unexpected producer failure"
    );
    assert_eq!(
        settled, initial_baseline,
        "GUARD_BLOCKED: original funding did not settle after owned cleanup"
    );
    if let Some((allocation, payment, charged)) = positive_bad_success {
        assert!(allocation.0 != 0 && allocation.1 > 0);
        assert_eq!(charged, LIMIT);
        panic!(
            "POSITIVE_NATIVE_CAP_BYPASS allocation={allocation:?} payment={payment:?} charged={charged} settled={settled}"
        );
    }
    assert!(expected_refusal, "GUARD_BLOCKED: no positive outcome");
}

#[test]
#[serial]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn independently_funded_dirty_pages_preserve_a_full_disjoint_clean_working_set() {
    let writer = MemoryAccountant::writer_budget(LIMIT).unwrap();
    let owner = writer.accountant();
    let baseline = owner.usage().used;
    let funder = writer.prepare_source_funder().unwrap();
    let original = writer.reserve_assembly_original(&funder).unwrap();
    let scope = StorageAdmissionScope::writer_original_native(&original).unwrap();
    let fixture = scope
        .run_native(|| {
            redb::CachePressureFixtureForTest::new()
                .map_err(|e| contextdb_core::Error::Other(e.to_string().into()))
        })
        .unwrap();
    let (capacity, index) = fixture.capacity_and_index();
    let reserve =
        crate::persistence::cache_reserve::original(&owner, capacity + index, false).unwrap();
    let controls = fixture.retain_reserve(reserve);
    scope
        .run_native(|| {
            fixture
                .warm()
                .map_err(crate::persistence::RedbPersistence::storage_error)
        })
        .unwrap();
    let warm = fixture.clean_resident();
    let warm_held = fixture.held();
    assert_eq!(
        warm, 8,
        "GUARD_BLOCKED: all eight distinct clean pages must be warm"
    );
    assert_eq!(
        warm_held.0, capacity,
        "GUARD_BLOCKED: clean cache must actually be full"
    );
    let dirty = scope.run_native(|| {
        fixture
            .dirty_disjoint()
            .map_err(crate::persistence::RedbPersistence::storage_error)
    });
    let after_dirty = fixture.clean_resident();
    let dirty_held = fixture.held();
    let dirty_used = owner.usage().used;
    let flushed = scope.run_native(|| {
        fixture
            .flush()
            .map_err(crate::persistence::RedbPersistence::storage_error)
    });
    let after_flush = fixture.clean_resident();
    let flush_held = fixture.held();
    drop(controls);
    drop(fixture);
    drop(scope);
    drop(original);
    drop(funder);
    let settled = owner.usage().used;
    eprintln!(
        "DISJOINT_PRESSURE capacity={capacity} index={index} warm={warm} warm_held={warm_held:?} after_dirty={after_dirty} dirty_held={dirty_held:?} dirty_used={dirty_used} after_flush={after_flush} flush_held={flush_held:?} settled={settled} baseline={baseline}"
    );
    dirty.expect("GUARD_BLOCKED: funded dirty construction");
    flushed.expect("GUARD_BLOCKED: flush succeeds");
    assert_eq!(
        settled, baseline,
        "GUARD_BLOCKED: fixture original credit must settle"
    );
    assert_eq!(
        after_dirty, warm,
        "dirty pages evicted disjoint clean serving pages"
    );
    assert_eq!(
        after_flush, warm,
        "bulk flush evicted disjoint clean serving pages"
    );
    assert!(
        dirty_held.0 <= capacity && flush_held.0 <= capacity,
        "clean cache stays in funded reserve"
    );
}
