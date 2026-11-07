//! Recorder support tests only: actual allocator/funding cuts, no database/helper/BPF.
use contextdb_core::coordination_test_observation as receipts;
use contextdb_core::read_memory::{ReadCredit, ReadMemoryBudget, ReadMemoryOwner, ReadShared};
use serial_test::serial;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

#[derive(Debug, Default)]
struct Budget {
    used: AtomicU64,
}
impl ReadMemoryBudget for Budget {
    fn try_reserve(
        &self,
        bytes: usize,
        _: &'static str,
        _: &'static str,
        _: &'static str,
    ) -> contextdb_core::Result<()> {
        self.used.fetch_add(bytes as u64, Ordering::SeqCst);
        Ok(())
    }
    fn release(&self, bytes: usize) {
        self.used.fetch_sub(bytes as u64, Ordering::SeqCst);
    }
    fn coordination_counter_for_test(&self) -> Option<receipts::FundingCounter<'_>> {
        Some(receipts::FundingCounter::U64(&self.used))
    }
}
fn credit<T>(owner: &ReadMemoryOwner) -> ReadCredit {
    ReadCredit::try_new_owned(
        owner.clone(),
        ReadShared::<T>::layout().size(),
        "test",
        "recorder",
        "fixture",
    )
    .unwrap()
}

#[test]
#[serial]
fn recorder_enrolls_nested_and_foreign_thread_constructors_and_real_cross_thread_free() {
    let budget = Arc::new(Budget::default());
    let owner = ReadMemoryOwner::from_legacy(budget.clone());
    let window = receipts::begin().unwrap();
    let loan = receipts::funding_loan(window, &owner).unwrap();
    let thread = receipts::attach(window, 17, 23).unwrap();
    let outer = credit::<u64>(&owner);
    let inner = credit::<u128>(&owner);
    let (outer, inner) = {
        let _outer = receipts::allocation(
            receipts::AllocationClass::HelperPlan,
            ReadShared::<u64>::layout(),
            &outer,
        );
        let inner = {
            let _inner = receipts::allocation(
                receipts::AllocationClass::WakePipe,
                ReadShared::<u128>::layout(),
                &inner,
            );
            ReadShared::try_prepare_native(inner)
                .unwrap()
                .publish(2_u128)
        };
        (
            ReadShared::try_prepare_native(outer)
                .unwrap()
                .publish(1_u64),
            inner,
        )
    };
    drop(thread);
    let creator = owner.clone();
    let other = std::thread::spawn(move || {
        let _thread = receipts::attach(window, 17, 24).unwrap();
        let invoice = credit::<u32>(&creator);
        let _scope = receipts::allocation(
            receipts::AllocationClass::TaskStart,
            ReadShared::<u32>::layout(),
            &invoice,
        );
        ReadShared::try_prepare_native(invoice)
            .unwrap()
            .publish(3_u32)
    })
    .join()
    .unwrap();
    std::thread::spawn(move || {
        drop(outer);
        drop(inner);
        drop(other);
    })
    .join()
    .unwrap();
    let (count, allocations, errors) = receipts::status(window).unwrap();
    assert_eq!(errors, 0);
    assert_eq!(allocations, 3);
    let mut frees = 0;
    let mut second_constructor_thread = false;
    for index in 0..count {
        let before = receipts::record(window, index).unwrap();
        if before.kind == 1 && before.thread == 2 {
            second_constructor_thread = true;
        }
        if before.kind == 3 {
            let after = receipts::record(window, index + 1).unwrap();
            assert_eq!(after.kind, 4);
            assert_eq!(before.constructor, after.constructor);
            assert_eq!(before.funding, after.funding);
            assert_eq!(before.allocator_generation, before.window);
            assert_ne!(before.allocator_ordinal, 0);
            assert_eq!(before.allocator_generation, after.allocator_generation);
            assert!(
                matches!(before.funding, receipts::FundingSample::U64(v) if v >= before.bytes as u64)
            );
            frees += 1;
        }
    }
    assert_eq!(frees, 3);
    assert!(second_constructor_thread);
    assert_eq!(budget.used.load(Ordering::SeqCst), 0);
    assert!(loan.finish());
    assert!(receipts::finish(window));
}

#[test]
#[serial]
fn recorder_binds_actual_allocator_null_and_preserves_the_refunded_failure_invoice() {
    let budget = Arc::new(Budget::default());
    let owner = ReadMemoryOwner::from_legacy(budget.clone());
    let window = receipts::begin().unwrap();
    let loan = receipts::funding_loan(window, &owner).unwrap();
    let thread = receipts::attach(window, 31, 32).unwrap();
    let layout = ReadShared::<[u64; 19]>::layout();
    crate::vector_read_memory::native_ownership_proof::arm_refuse_one_live_layout(layout);
    let (result, _, _) = super::native_requests_for_test(|| {
        let invoice = credit::<[u64; 19]>(&owner);
        let _scope = receipts::allocation(receipts::AllocationClass::HelperPlan, layout, &invoice);
        ReadShared::<[u64; 19]>::try_prepare_native(invoice)
    });
    assert!(result.is_err());
    assert!(!crate::vector_read_memory::native_ownership_proof::refusal_armed_for_test());
    drop(thread);
    let (count, allocations, errors) = receipts::status(window).unwrap();
    assert_eq!((allocations, errors), (1, 0));
    assert_eq!(count, 3);
    let entered = receipts::record(window, 1).unwrap();
    assert_eq!(entered.kind, 12);
    assert_eq!(entered.allocator_ordinal, 1);
    let returned = receipts::record(window, 2).unwrap();
    assert_eq!(returned.kind, 2);
    assert_eq!(returned.address, 0);
    assert_eq!(returned.result, -1);
    assert!(!returned.system_called);
    assert_eq!(budget.used.load(Ordering::SeqCst), 0);
    assert!(loan.finish());
    assert!(receipts::finish(window));
}

#[test]
#[serial]
fn recorder_rejects_wrong_layout_without_a_fake_free() {
    let budget = Arc::new(Budget::default());
    let owner = ReadMemoryOwner::from_legacy(budget.clone());
    let window = receipts::begin().unwrap();
    let loan = receipts::funding_loan(window, &owner).unwrap();
    let thread = receipts::attach(window, 41, 42).unwrap();
    let invoice = credit::<u64>(&owner);
    let block = {
        let _scope = receipts::allocation(
            receipts::AllocationClass::HelperPlan,
            ReadShared::<[u64; 91]>::layout(),
            &invoice,
        );
        ReadShared::try_prepare_native(invoice)
            .unwrap()
            .publish(4_u64)
    };
    drop(block);
    drop(thread);
    let (count, _, errors) = receipts::status(window).unwrap();
    assert_ne!(errors & 16, 0);
    assert!((0..count).all(|i| !matches!(receipts::record(window, i).unwrap().kind, 3 | 4)));
    assert_eq!(budget.used.load(Ordering::SeqCst), 0);
    drop(loan);
    assert!(receipts::abort(window));
}
