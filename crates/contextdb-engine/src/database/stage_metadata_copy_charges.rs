//! Capacity custody for the enlarged inline received-schema metadata charges.
//! Legacy admits the element-layout delta; native assembly carries an
//! explicit original for the full backing. Individual payload/sole prepaid Box
//! owners retain their existing independent custody.
use super::{Error, Result, StagedCopyCharge};
use crate::memory_accounting::{ControlCredit, PendingControlRefund, WriterUse};
use std::alloc::Layout;
use std::mem::ManuallyDrop;

// Field-for-field baseline database.rs StagedCopyCharge. Never constructed: its
// compiler layout is the same-capacity baseline, not a guessed word count.
#[allow(
    dead_code,
    reason = "LAYOUT-ONLY: never constructed; its layout sizes production memory charges"
)]
struct BaselineStagedCopyCharge {
    accountant: crate::memory_accounting::AccountantHandle,
    bytes: usize,
}

fn capacity_error() -> Error {
    Error::Other(("received metadata charge allocation capacity overflow".to_string()).into())
}

pub(super) struct MetadataCopyCharges {
    charges: ManuallyDrop<Vec<StagedCopyCharge>>,
    backing: Option<ControlCredit>,
    accountant: ManuallyDrop<crate::memory_accounting::AccountantHandle>,
    writer_use: ManuallyDrop<Option<WriterUse>>,
    // Native backing owns its full layout and authentic seed, independently
    // from the Legacy baseline correction. Automatic field drop follows cleanup.
    native_backing: Option<crate::memory_accounting::OwnedMemoryReservation>,
}

impl MetadataCopyCharges {
    pub(super) fn new(accountant: &crate::memory_accounting::AccountantHandle) -> Self {
        Self {
            charges: ManuallyDrop::new(Vec::new()),
            backing: None,
            accountant: ManuallyDrop::new(accountant.clone()),
            writer_use: ManuallyDrop::new(WriterUse::retain_if_writer(accountant)),
            native_backing: None,
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn from_original(
        original: &crate::memory_accounting::OwnedMemoryReservation,
    ) -> Result<Self> {
        let seed = crate::memory_accounting::reserve_preparation_from_original(
            original,
            0,
            "sync",
            "received_metadata_controls",
            "Raise MEMORY_LIMIT to prepare received metadata.",
        )?;
        Ok(Self {
            charges: ManuallyDrop::new(Vec::new()),
            backing: None,
            accountant: ManuallyDrop::new(crate::memory_accounting::AccountantHandle::clone(
                original.accountant(),
            )),
            writer_use: ManuallyDrop::new(None),
            native_backing: Some(seed),
        })
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn from_original_array<const N: usize>(
        original: &crate::memory_accounting::OwnedMemoryReservation,
        charges: [StagedCopyCharge; N],
    ) -> Result<Self> {
        let mut owner = Self::from_original(original)?;
        owner.grow_to(N)?;
        for charge in charges {
            owner.charges.push(charge);
        }
        Ok(owner)
    }

    /// Preserve vec![a, b, c]'s exact initial capacity in the no-op stage.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
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

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
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

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn grow_to(&mut self, capacity: usize) -> Result<()> {
        if capacity == 0 || std::mem::size_of::<StagedCopyCharge>() == 0 {
            // Vec's zero-capacity and ZST paths make no physical allocation.
            return Ok(());
        }
        let layout = Layout::array::<StagedCopyCharge>(capacity).map_err(|_| capacity_error())?;
        if let Some(original) = self.native_backing.as_ref() {
            let admission = crate::memory_accounting::reserve_preparation_from_original(
                original,
                layout.size(),
                "sync",
                "received_metadata_charge_vec_growth",
                "Raise MEMORY_LIMIT to prepare received metadata.",
            )?;
            let mut replacement = Self::from_original(original)?;
            replacement.native_backing = Some(admission);
            replacement.charges = ManuallyDrop::new(Vec::with_capacity(capacity));
            if replacement.charges.capacity() != capacity {
                return Err(capacity_error());
            }
            replacement.charges.append(&mut self.charges);
            std::mem::swap(&mut self.charges, &mut replacement.charges);
            std::mem::swap(&mut self.native_backing, &mut replacement.native_backing);
            // The old empty Vec physically deallocates in existing cleanup
            // before native_backing's automatic final original retirement.
            drop(replacement);
            return Ok(());
        }
        let baseline =
            Layout::array::<BaselineStagedCopyCharge>(capacity).map_err(|_| capacity_error())?;
        let correction = layout
            .size()
            .checked_sub(baseline.size())
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
