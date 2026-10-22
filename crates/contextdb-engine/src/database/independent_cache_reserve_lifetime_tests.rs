//! Independent tests enter through the real engine open/close and maintenance
//! recycler. No replacement parent or stand-in atomic pays these allocations.
use super::*;
use crate::memory_accounting::{ControlCredit, OwnedMemoryReservation};
use crate::persistence::content_identity::proof_allocation_oracle_tests as oracle;

struct FreeWatch;
impl Drop for FreeWatch {
    fn drop(&mut self) {
        oracle::disarm_source_free_watch();
    }
}
struct ProcessCeiling;
impl Drop for ProcessCeiling {
    fn drop(&mut self) {
        crate::memory_accounting::set_process_ceiling_for_test(None);
    }
}
fn actual_funding(accountant: &crate::memory_accounting::AccountantHandle, residue: bool) -> usize {
    if residue {
        crate::memory_accounting::process_aggregate_bytes_for_test()
    } else {
        accountant.usage().used
    }
}

// Every pointer must name its own actual current payer; a plausible aggregate
// alone cannot qualify the overlap. SourceCounted is deliberately NOT blessed
// by cache capacity: that branch needs a separate authentic source-invoice proof.
fn paid_pages(pages: &[&redb::CachePageForTest], controls: &redb::CacheReserveOwner) {
    let counters = controls.observation_for_test();
    let mut expected = [0usize; 2];
    for (index, page) in pages.iter().enumerate() {
        let allocation = page.allocation();
        for other in &pages[..index] {
            assert_ne!(allocation.0, other.allocation().0);
        }
        let payment = page.payment();
        assert_eq!(
            payment.0, "cache",
            "non-cache payer needs its authentic source invoice proof: {allocation:?} {payment:?}"
        );
        let payer = counters
            .iter()
            .position(|counter| counter.0 == payment.1)
            .expect("each PageBlock must retain an ORIGINAL actual R/W control");
        assert_eq!(
            payment.2, allocation.1,
            "payment covers the actual complete native Layout"
        );
        expected[payer] += allocation.1;
    }
    for index in 0..2 {
        assert!(counters[index].1 >= expected[index]);
    }
    assert!(counters[0].1 + counters[1].1 >= expected[0] + expected[1]);
    // All payer locks are gone; print observations only after measuring them.
    for page in pages {
        let allocation = page.allocation();
        let payment = page.payment();
        eprintln!(
            "CACHE_CURRENT_PAYMENT allocation={allocation:?} payment={payment:?} counters={counters:?}"
        );
    }
}

fn physical_parent_row(residue: bool, recycle: bool) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("physical-reserve.db");
    let db = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            memory_limit: if residue {
                None
            } else {
                Some(128 * 1024 * 1024)
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .unwrap();
    let accountant = crate::memory_accounting::AccountantHandle::clone(&db.finalization.accountant);
    // Keep the real registration independently alive, so final-reserve delta
    // cannot accidentally include unrelated writer-registration teardown.
    let _writer_pin = crate::memory_accounting::WriterUse::retain_if_writer(&accountant).unwrap();
    let persistence = db.persistence.as_ref().unwrap().clone();
    let (capacity, fixed) = persistence.cache_installation_for_test();
    assert!(capacity > 0 && fixed > capacity);
    assert_eq!(persistence.cache_funded_bytes(), Some(fixed));
    if residue {
        assert_eq!(persistence.cache_charge(), 0);
        assert_eq!(persistence.cache_stripe_residue_bytes(), fixed);
    } else {
        assert_eq!(persistence.cache_charge(), fixed);
        assert_eq!(persistence.cache_stripe_residue_bytes(), 0);
    }
    let reserve = persistence.original_cache_reserve_for_test();
    assert_ne!(reserve.0, 0);
    let invoice = fixed + reserve.1;
    let controls = persistence.original_cache_controls_for_test();
    let identities = controls.observation_for_test().map(|v| v.0);
    let (cached, dirty, write_control) = persistence
        .with_db(|raw| {
            assert_eq!(raw.cache_controls_for_test().map(|v| v.0), identities);
            raw.lifetime_aliases_for_test()
                .map_err(RedbPersistence::storage_error)
        })
        .unwrap();
    let cached_clone = cached.clone();
    let dirty_clone = dirty.clone();
    let expected = cached.bytes().to_vec();
    assert_eq!(dirty.bytes(), expected);
    assert_eq!(write_control.bytes(), expected);
    let old_cached = cached.allocation();
    let old_dirty = dirty.allocation();
    assert_ne!(old_cached.0, old_dirty.0);
    assert_eq!(cached_clone.allocation(), old_cached);
    assert_eq!(dirty_clone.allocation(), old_dirty);
    let occupancy = controls.observation_for_test();
    assert!(occupancy[0].1 >= old_cached.1);
    assert!(occupancy[1].1 >= old_dirty.1);

    paid_pages(&[&cached, &dirty], &controls);
    let before_recycle_payment = dirty.payment();
    let mut next_generation = None;
    if recycle {
        let before = actual_funding(&accountant, residue);
        assert!(
            persistence
                .recycle_cache_handle_for_test()
                .unwrap()
                .is_some(),
            "actual maintenance recycler must replace the native handle"
        );
        assert_eq!(
            persistence.original_cache_reserve_for_test(),
            reserve,
            "recycle must retain the original actual ReserveBlock"
        );
        assert_eq!(persistence.cache_funded_bytes(), Some(fixed));
        assert_eq!(
            actual_funding(&accountant, residue),
            before,
            "preparation overlap refunds; no second fixed reserve is charged"
        );
        paid_pages(&[&cached, &dirty], &controls);
        let (fresh, fresh_dirty, fresh_control) = persistence
            .with_db(|raw| {
                assert_eq!(
                    raw.cache_controls_for_test().map(|v| v.0),
                    identities,
                    "new handle must use the actual old occupancy controls"
                );
                raw.lifetime_aliases_for_test()
                    .map_err(RedbPersistence::storage_error)
            })
            .unwrap();
        assert_ne!(fresh.allocation().0, old_cached.0);
        assert_ne!(fresh_dirty.allocation().0, old_dirty.0);
        let overlap = controls.observation_for_test();
        assert!(overlap[0].1 >= old_cached.1 + fresh.allocation().1);
        paid_pages(&[&cached, &dirty, &fresh, &fresh_dirty], &controls);
        let after_recycle_payment = dirty.payment();
        eprintln!(
            "CACHE_RECYCLE_HANDOFF pointer={} before={before_recycle_payment:?} after={after_recycle_payment:?}",
            old_dirty.0
        );
        assert_eq!(cached.bytes(), expected);
        assert_eq!(dirty.bytes(), expected);
        let fresh_expected = fresh.bytes().to_vec();
        assert_eq!(fresh_dirty.bytes(), fresh_expected);
        assert_eq!(fresh_control.bytes(), fresh_expected);
        next_generation = Some((fresh, fresh_dirty, fresh_control, fresh_expected));
    }

    paid_pages(&[&cached, &dirty], &controls);
    db.close().unwrap();
    paid_pages(&[&cached, &dirty], &controls);
    drop(db);
    drop(persistence);
    assert!(
        actual_funding(&accountant, residue) >= invoice,
        "close cannot refund the retained physical cache's original reserve"
    );
    assert_eq!(cached.bytes(), expected);
    assert_eq!(dirty.bytes(), expected);
    assert_eq!(write_control.bytes(), expected);
    if let Some((fresh, fresh_dirty, fresh_control, fresh_expected)) = next_generation {
        assert_eq!(fresh.bytes(), fresh_expected);
        assert_eq!(fresh_dirty.bytes(), fresh_expected);
        assert_eq!(fresh_control.bytes(), fresh_expected);
        drop(fresh);
        drop(fresh_dirty);
        drop(fresh_control);
    }
    paid_pages(&[&cached, &dirty], &controls);
    let counter = if residue {
        0
    } else {
        accountant.writer_registration_observation_for_test().4
    };
    let _watch = FreeWatch;
    oracle::watch_exact_free_for_test(old_cached, counter);
    drop(cached);
    assert_eq!(
        oracle::source_free_for_test(1)[0].2,
        0,
        "first clone release cannot physically free the original backing"
    );
    let funded = actual_funding(&accountant, residue);
    let occupied = controls.observation_for_test()[0].1;
    drop(cached_clone);
    assert_eq!(
        oracle::source_free_for_test(1)[0],
        (funded, funded, 1),
        "original reserve survives BOTH sides of actual cached backing free"
    );
    assert_eq!(
        controls.observation_for_test()[0].1,
        occupied - old_cached.1
    );

    oracle::watch_exact_free_for_test(old_dirty, counter);
    drop(dirty);
    drop(dirty_clone);
    assert_eq!(
        oracle::source_free_for_test(1)[0].2,
        0,
        "the real detached writable control still holds the dirty backing"
    );
    let funded = actual_funding(&accountant, residue);
    drop(write_control);
    assert_eq!(
        oracle::source_free_for_test(1)[0],
        (funded, funded, 1),
        "dirty/map/control teardown must precede fixed reserve refund"
    );
    assert_eq!(controls.observation_for_test().map(|v| v.1), [0, 0]);
    assert!(
        actual_funding(&accountant, residue) >= invoice,
        "actual occupancy-control alias alone retains the original reserve"
    );

    let (control_size, control_align) = controls.control_layout_for_test();
    let counter_backings = controls.observation_for_test();
    oracle::watch_exact_frees_for_test(
        &[
            (counter_backings[0].0, control_size, control_align),
            (counter_backings[1].0, control_size, control_align),
            reserve,
        ],
        counter,
    );
    let held = actual_funding(&accountant, residue);
    let _ceiling = ProcessCeiling;
    if residue {
        crate::memory_accounting::set_process_ceiling_for_test(Some(held));
        assert!(
            ControlCredit::admit_open_residue_native(
                &accountant,
                invoice,
                "independent_same_cap_readmission"
            )
            .is_err()
        );
    } else {
        accountant.set_budget(Some(held)).unwrap();
        assert!(
            OwnedMemoryReservation::try_new_for_native(
                accountant.clone(),
                invoice,
                "test",
                "independent_same_cap_readmission",
                "Release held backing."
            )
            .is_err()
        );
    }
    assert_eq!(actual_funding(&accountant, residue), held);
    drop(controls);
    let cuts = oracle::source_free_for_test(3);
    assert_eq!(
        &cuts[..3],
        &[(held, held, 1); 3],
        "BOTH occupancy controls and the original reserve keep all invoices through System.dealloc"
    );
    assert_eq!(
        actual_funding(&accountant, residue),
        held - invoice,
        "exactly one fixed reserve and its own control are refunded after last free"
    );
    if residue {
        let readmitted = ControlCredit::admit_open_residue_native(
            &accountant,
            invoice,
            "independent_same_cap_readmission",
        )
        .unwrap();
        assert_eq!(actual_funding(&accountant, residue), held);
        drop(readmitted);
    } else {
        let readmitted = OwnedMemoryReservation::try_new_for_native(
            accountant.clone(),
            invoice,
            "test",
            "independent_same_cap_readmission",
            "Release held backing.",
        )
        .unwrap();
        assert_eq!(actual_funding(&accountant, residue), held);
        drop(readmitted);
    }
    assert_eq!(actual_funding(&accountant, residue), held - invoice);
    assert_eq!(accountant.underflow_count_for_test(), 0);
}

#[test]
#[serial_test::serial]
fn declared_writer_reserve_survives_cached_dirty_control_aliases_and_close() {
    physical_parent_row(false, false);
}
#[test]
#[serial_test::serial]
fn process_residue_reserve_survives_cached_dirty_control_aliases_and_close() {
    physical_parent_row(true, false);
}
#[test]
#[serial_test::serial]
fn declared_writer_recycle_keeps_old_and_new_generations_on_one_physical_reserve() {
    physical_parent_row(false, true);
}
#[test]
#[serial_test::serial]
fn process_residue_recycle_keeps_old_and_new_generations_on_one_physical_reserve() {
    physical_parent_row(true, true);
}
