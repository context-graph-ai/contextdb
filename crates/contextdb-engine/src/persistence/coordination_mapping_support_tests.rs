//! Actual mapping receipts only; never spawns a raw helper or opens a database.
use super::*;
use contextdb_core::coordination_test_observation as receipts;
use contextdb_core::read_memory::ReadMemoryBudget;
use std::sync::atomic::{AtomicU64, Ordering};
#[derive(Debug, Default)]
struct Budget {
    used: AtomicU64,
    projections: AtomicU64,
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
        self.projections.fetch_add(1, Ordering::SeqCst);
        Some(receipts::FundingCounter::U64(&self.used))
    }
}
#[test]
#[serial_test::serial]
fn actual_mapping_and_unmap_return_precede_original_invoice_settlement() {
    let budget = Arc::new(Budget::default());
    let owner = ReadMemoryOwner::from_legacy(budget.clone());
    let window = receipts::begin().unwrap();
    let loan = receipts::funding_loan(window, &owner).unwrap();
    let projection_count = budget.projections.load(Ordering::SeqCst);
    let thread = receipts::attach(window, 71, 72).unwrap();
    let stack = HelperStack::map(wait_credit(&owner, HELPER_STACK_BYTES).unwrap()).unwrap();
    let address = stack.address as usize;
    drop(stack);
    drop(thread);
    let (count, mappings, errors) = receipts::status(window).unwrap();
    assert_eq!((count, mappings, errors), (4, 1, 0));
    for index in 0..count {
        let record = receipts::record(window, index).unwrap();
        assert_eq!(record.kind, 5 + index as u8);
        assert_eq!(record.bytes, HELPER_STACK_BYTES);
        assert_eq!(
            record.funding,
            receipts::FundingSample::U64(HELPER_STACK_BYTES as u64)
        );
        if index > 0 {
            assert_eq!(record.address, address);
        }
        assert_eq!(record.result, 0);
        assert_eq!(record.errno, None);
    }
    assert_eq!(budget.used.load(Ordering::SeqCst), 0);
    assert_eq!(
        budget.projections.load(Ordering::SeqCst),
        projection_count,
        "mapping recorder must not call provider code"
    );
    assert!(loan.finish());
    assert!(receipts::finish(window));
}
