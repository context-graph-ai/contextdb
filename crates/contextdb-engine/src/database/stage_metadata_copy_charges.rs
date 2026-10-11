//! Capacity custody for the enlarged inline received-schema metadata charges.
//! Only CDB-127's element-layout delta is admitted; pre2 backing remains outside
//! this correction, and individual payload/sole prepaid Box owners are unchanged.
use super::{Error, Result, StagedCopyCharge};
#[cfg(all(test, feature = "test-seams"))]
use super::MemoryAccountant;
#[cfg(all(test, feature = "test-seams"))]
use std::sync::Arc;
use crate::memory_accounting::{ControlCredit, PendingControlRefund, WriterUse};
use std::alloc::Layout;
use std::mem::ManuallyDrop;

// Field-for-field pre2 database.rs StagedCopyCharge. Never constructed: its
// compiler layout is the same-capacity baseline, not a guessed word count.
#[allow(dead_code)]
struct Pre2StagedCopyCharge {
    accountant: crate::memory_accounting::AccountantHandle,
    bytes: usize,
}

fn capacity_error() -> Error {
    Error::Other("received metadata charge allocation capacity overflow".to_string())
}

pub(super) struct MetadataCopyCharges {
    charges: ManuallyDrop<Vec<StagedCopyCharge>>,
    backing: Option<ControlCredit>,
    accountant: ManuallyDrop<crate::memory_accounting::AccountantHandle>,
    writer_use: ManuallyDrop<Option<WriterUse>>,
}

impl MetadataCopyCharges {
    pub(super) fn new(accountant: &crate::memory_accounting::AccountantHandle) -> Self {
        Self {
            charges: ManuallyDrop::new(Vec::new()),
            backing: None,
            accountant: ManuallyDrop::new(accountant.clone()),
            writer_use: ManuallyDrop::new(WriterUse::retain_if_writer(accountant)),
        }
    }

    /// Preserve vec![a, b, c]'s exact initial capacity in the no-op stage.
    pub(super) fn from_array<const N: usize>(
        accountant: &crate::memory_accounting::AccountantHandle,
        charges: [StagedCopyCharge; N],
    ) -> Result<Self> {
        let mut owner = Self::new(accountant);
        owner.grow_to(N)?;
        for charge in charges {
            // Already funded and allocated; this cannot cause another growth.
            owner.charges.push(charge);
        }
        Ok(owner)
    }

    pub(super) fn push(&mut self, charge: StagedCopyCharge) -> Result<()> {
        if self.charges.len() == self.charges.capacity() {
            let required = self
                .charges
                .len()
                .checked_add(1)
                .ok_or_else(capacity_error)?;
            // Pinned Rust 1.94.1 alloc/raw_vec: the same MIN_NON_ZERO_CAP and
            // grow_amortized capacity selection as this family's native push.
            let minimum = match std::mem::size_of::<StagedCopyCharge>() {
                0 => return Err(capacity_error()), // a full ZST Vec has usize::MAX entries
                1 => 8,
                2..=1024 => 4,
                _ => 1,
            };
            let doubled = self
                .charges
                .capacity()
                .checked_mul(2)
                .ok_or_else(capacity_error)?;
            self.grow_to(doubled.max(required).max(minimum))?;
        }
        self.charges.push(charge);
        Ok(())
    }

    fn grow_to(&mut self, capacity: usize) -> Result<()> {
        if capacity == 0 || std::mem::size_of::<StagedCopyCharge>() == 0 {
            // Vec's zero-capacity and ZST paths make no physical allocation.
            return Ok(());
        }
        let layout = Layout::array::<StagedCopyCharge>(capacity).map_err(|_| capacity_error())?;
        let pre2 = Layout::array::<Pre2StagedCopyCharge>(capacity).map_err(|_| capacity_error())?;
        let correction = layout
            .size()
            .checked_sub(pre2.size())
            .ok_or_else(capacity_error)?;
        // Keep the original accountant and writer custody through admission,
        // failed construction and old backing destruction. No payer retargeting.
        let mut replacement = Self::new(&self.accountant);
        replacement.backing = Some(ControlCredit::admit(
            &self.accountant,
            correction,
            "received_metadata_charge_vec_growth",
        )?);
        // ENTIRE new scoped liability is live while the old one remains live.
        // Pinned Global RawVec requests exactly Layout::array(final, capacity).
        replacement.charges = ManuallyDrop::new(Vec::with_capacity(capacity));
        // The separate destination already has enough capacity. append moves
        // once and runs no element code; allocation/refusal precedes this move.
        replacement.charges.append(&mut self.charges);
        std::mem::swap(&mut self.charges, &mut replacement.charges);
        std::mem::swap(&mut self.backing, &mut replacement.backing);
        // This now owns the EMPTY old allocation: physical free, its extra
        // accountant/use teardown, then old scoped refund, in that order.
        drop(replacement);
        Ok(())
    }
}

struct MetadataCopyChargesCleanup {
    source: *mut MetadataCopyCharges,
    next: u8,
    refund: Option<PendingControlRefund>,
}

impl MetadataCopyChargesCleanup {
    fn finish(&mut self) {
        unsafe {
            if self.next == 0 {
                // Remove before Drop. If one payload unwinds, guard Drop
                // finishes every remaining payload exactly once, then frees
                // the still-owned empty backing before its scoped refund.
                while let Some(charge) = (*self.source).charges.pop() {
                    drop(charge);
                }
                self.next = 1;
            }
            while self.next < 5 {
                let step = self.next;
                self.next += 1;
                match step {
                    1 => ManuallyDrop::drop(&mut (*self.source).charges),
                    2 => ManuallyDrop::drop(&mut (*self.source).accountant),
                    3 => ManuallyDrop::drop(&mut (*self.source).writer_use),
                    4 => drop(self.refund.take()),
                    _ => unreachable!(),
                }
            }
        }
    }
}

impl Drop for MetadataCopyChargesCleanup {
    fn drop(&mut self) {
        self.finish();
    }
}

impl Drop for MetadataCopyCharges {
    fn drop(&mut self) {
        let refund = self.backing.take().map(ControlCredit::prepare);
        let mut cleanup = MetadataCopyChargesCleanup {
            source: self,
            next: 0,
            refund,
        };
        cleanup.finish();
    }
}

#[cfg(all(test, feature = "test-seams"))]
pub(super) fn initialized_rows_for_test() {
    use crate::persistence::content_identity::proof_allocation_oracle_tests::{measured,
        last_request_for_test, watch_registry_free_for_test, registry_free_observation_for_test};
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let writer = crate::memory_accounting::WriterBudget::from_accountant(accountant.clone()).unwrap();
    let registration = accountant.writer_registration_observation_for_test();
    let baseline = accountant.usage().used;
    let final_element = Layout::new::<StagedCopyCharge>();
    let pre2_element = Layout::new::<Pre2StagedCopyCharge>();
    let delta = final_element.size().checked_sub(pre2_element.size()).unwrap();
    let charge = || StagedCopyCharge::admit(&accountant, accountant.clone(), 0, "metadata_initialized_proof").unwrap();
    let mut charges = MetadataCopyCharges::new(&accountant);
    assert_eq!(charges.charges.capacity(), 0);
    assert_eq!(accountant.usage().used, baseline);
    let ((), _, _) = measured(|| charges.push(charge()).unwrap());
    let first_request = last_request_for_test(false);
    assert_eq!(charges.charges.capacity(), 4);
    assert_eq!((first_request.0, first_request.1), (final_element.size() * 4, final_element.align()));
    assert_eq!(accountant.usage().used, baseline + delta * 4);
    for _ in 0..3 { charges.push(charge()).unwrap(); }
    let old = charges.charges.as_ptr() as usize;
    watch_registry_free_for_test(old, registration.4);
    let ((), _, _) = measured(|| charges.push(charge()).unwrap());
    let growth_request = last_request_for_test(false);
    assert_eq!(charges.charges.capacity(), 8);
    assert_eq!((growth_request.0, growth_request.1), (final_element.size() * 8, final_element.align()));
    assert_eq!(registry_free_observation_for_test(true), (baseline + delta * 12, baseline + delta * 12, true), "old backing physically frees with whole old and whole new correction live");
    assert_eq!(accountant.usage().used, baseline + delta * 8);
    let final_pointer = charges.charges.as_ptr() as usize;
    watch_registry_free_for_test(final_pointer, registration.4);
    drop(charges);
    assert_eq!(registry_free_observation_for_test(true), (baseline + delta * 8, baseline + delta * 8, true), "final backing physically frees before its scoped refund");
    assert_eq!(accountant.usage().used, baseline);
    let (exact, _, _) = measured(|| MetadataCopyCharges::from_array(&accountant, [charge(), charge(), charge()]).unwrap());
    assert_eq!(exact.charges.capacity(), 3);
    let exact_request = last_request_for_test(false);
    assert_eq!((exact_request.0, exact_request.1), (final_element.size() * 3, final_element.align()));
    assert_eq!(accountant.usage().used, baseline + delta * 3);
    drop(exact);
    assert_eq!(accountant.usage().used, baseline);
    drop(writer);
    assert_eq!(accountant.usage().used, 0);
    assert_eq!(accountant.underflow_count_for_test(), 0);
    println!("CDB127_R1_ROW metadata_vec_initialized final_element={} pre2_element={} align={} first_capacity=4 growth_capacity=8 exact_capacity=3 old_new_overlap=true old_final_physical_free_before_refund=true original_funder=true", final_element.size(), pre2_element.size(), final_element.align());
}
