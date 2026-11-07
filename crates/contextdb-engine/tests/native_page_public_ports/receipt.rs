//! Independent scalar consumer of the real producer's deferred events.
//! A source-counted occupancy decrement never settles a genuine funder.
use redb::native_page_observations::{
    NativePageBackingForTest as Backing, NativePageCustodyEventForTest as Event,
    NativePagePaymentForTest as Payment,
};
use std::collections::BTreeMap;
#[derive(Clone, Copy)]
struct Page {
    backing: Backing,
    payment: Payment,
    before_free: bool,
    freed: bool,
    ordinal: usize,
    failed_read: bool,
}
struct FailedRead {
    allocation: usize,
    ordinal: usize,
    receipt: usize,
    freed: bool,
    refunded: bool,
}
#[derive(Default)]
pub struct Ledger {
    failed_reads: Vec<FailedRead>,
    pages: BTreeMap<usize, Page>,
    reserves: BTreeMap<usize, (usize, usize, bool)>,
    reserve_process_only: BTreeMap<usize, bool>,
    sources: BTreeMap<usize, (usize, usize, bool)>,
    pub allocations: usize,
    pub physical_frees: usize,
    pub installs: usize,
    pub rejections: usize,
    pub evictions: usize,
    pub source_refunds: usize,
    pub occupancy_releases: usize,
    pub unnamed_bytes: usize,
}
impl Ledger {
    fn funding(&self, payment: Payment, backing: Backing) -> bool {
        if let Some(receipt) = payment.source_receipt {
            return receipt != 0
                && payment.original_funder.is_some_and(|id| id != 0)
                && payment.charged_bytes >= backing.layout.size();
        }
        payment.fixed_cache_receipt.is_some_and(|receipt| {
            self.reserves
                .get(&receipt)
                .is_some_and(|(owner, bytes, released)| {
                    *owner != 0 && !released && *bytes >= backing.layout.size()
                })
        }) && payment.fixed_cache_counter.is_some_and(|id| id != 0)
            && payment.charged_bytes >= backing.layout.size()
    }
    fn require_funding(&mut self, backing: Backing, payment: Payment) {
        if !self.funding(payment, backing) {
            self.unnamed_bytes += backing.layout.size();
        }
        assert_eq!(
            self.unnamed_bytes, 0,
            "every native physical backing has a genuine original payer, from load through release"
        );
    }
    pub fn observe(&mut self, event: Event) {
        match event {
            Event::FixedReserveAdmitted {
                receipt,
                original_funder,
                process_only,
                bytes,
                control,
                ..
            } => {
                assert!(receipt != 0 && original_funder != 0);
                assert!(bytes >= control.size());
                self.reserve_process_only.insert(receipt, process_only);
                if let Some((_, _, released)) = self
                    .reserves
                    .insert(receipt, (original_funder, bytes, false))
                {
                    assert!(released, "a still-live reserve cannot be admitted twice");
                }
            }
            Event::Allocated { backing, payment } => {
                if let Some(receipt) = payment.source_receipt
                    && let Some((_, _, released)) = self.reserves.get(&receipt)
                {
                    assert!(
                        *released,
                        "a source receipt cannot alias a live fixed reserve"
                    );
                    self.reserves.remove(&receipt);
                }
                assert!(backing.allocation != 0 && backing.payload >= backing.allocation);
                assert!(backing.layout.size() >= backing.payload_bytes);
                assert!(
                    backing.payload - backing.allocation + backing.payload_bytes
                        <= backing.layout.size()
                );
                assert_eq!(backing.allocation % backing.layout.align(), 0);
                if let Some(receipt) = payment.source_receipt {
                    let owner = payment
                        .original_funder
                        .unwrap_or_else(|| panic!("genuine source funder must join its original admission: backing={backing:?} payment={payment:?}"));
                    if let Some((_, _, released)) = self
                        .sources
                        .insert(receipt, (owner, backing.layout.size(), false))
                    {
                        assert!(released, "one source receipt cannot pay twice while live");
                    }
                }
                self.require_funding(backing, payment);
                if let Some(old) = self.pages.insert(
                    backing.allocation,
                    Page {
                        backing,
                        payment,
                        before_free: false,
                        freed: false,
                        ordinal: self.allocations,
                        failed_read: false,
                    },
                ) {
                    assert!(
                        old.freed,
                        "the same physical backing cannot be allocated twice while live"
                    );
                }
                self.allocations += 1;
            }
            Event::PaymentTransferred { backing, old, new } => {
                self.require_funding(backing, old);
                self.require_funding(backing, new);
                let page = self
                    .pages
                    .get_mut(&backing.allocation)
                    .expect("transfer refers to a real observed allocation");
                assert!(!page.freed && !page.before_free);
                assert_eq!(page.payment.source_receipt, old.source_receipt);
                assert_eq!(page.payment.fixed_cache_receipt, old.fixed_cache_receipt);
                page.payment = new;
                for (&receipt, &(_, reserve, released)) in &self.reserves {
                    if !released {
                        let held = self
                            .pages
                            .values()
                            .filter(|page| {
                                !page.freed
                                    && page.payment.source_receipt.is_none()
                                    && page.payment.fixed_cache_receipt == Some(receipt)
                            })
                            .map(|page| page.backing.layout.size())
                            .sum::<usize>();
                        assert!(
                            held <= reserve,
                            "all physical backings share the one actual fixed reserve"
                        );
                    }
                }
            }
            Event::Installed { allocation, .. } => {
                assert!(!self.pages[&allocation].freed);
                assert!(
                    !self.pages[&allocation].failed_read,
                    "a failed physical backing is never published into the serving cache"
                );
                self.installs += 1;
            }
            Event::Rejected { allocation, .. } => {
                assert!(!self.pages[&allocation].freed);
                self.rejections += 1;
            }
            Event::MapHandleEvicted {
                allocation,
                strong_handles,
                ..
            } => {
                assert!(!self.pages[&allocation].freed);
                assert!(strong_handles > 0);
                self.evictions += 1;
            }
            Event::BeforePhysicalFree { backing, payment } => {
                self.require_funding(backing, payment);
                let page = self
                    .pages
                    .get_mut(&backing.allocation)
                    .expect("physical free refers to a real allocation");
                assert!(!page.before_free && !page.freed);
                assert_eq!(page.backing.payload, backing.payload);
                assert_eq!(page.backing.layout, backing.layout);
                page.before_free = true;
            }
            Event::AfterPhysicalFree { backing, payment } => {
                self.require_funding(backing, payment);
                let page = self
                    .pages
                    .get_mut(&backing.allocation)
                    .expect("physical free refers to a real allocation");
                assert!(page.before_free && !page.freed);
                assert_eq!(page.backing.layout, backing.layout);
                page.freed = true;
                if page.failed_read {
                    let failure = self
                        .failed_reads
                        .iter_mut()
                        .find(|failure| {
                            failure.allocation == backing.allocation
                                && failure.ordinal == page.ordinal
                        })
                        .expect("the actual fault identifies this allocation generation");
                    assert!(!failure.freed);
                    failure.freed = true;
                }
                self.physical_frees += 1;
            }
            Event::ColdReadFailed { attempt } => {
                let page = self
                    .pages
                    .get_mut(&attempt.backing.allocation)
                    .expect("a cold error refers to the producer's live paid allocation");
                assert!(!page.freed && !page.before_free && !page.failed_read);
                assert_eq!(page.backing.payload, attempt.backing.payload);
                assert_eq!(page.backing.layout, attempt.backing.layout);
                assert_eq!(page.backing.payload_bytes, attempt.backing.payload_bytes);
                assert_eq!(page.payment.source_receipt, attempt.source_receipt);
                let receipt = attempt
                    .source_receipt
                    .expect("unpublished cold backing keeps its actual source invoice");
                page.failed_read = true;
                self.failed_reads.push(FailedRead {
                    allocation: attempt.backing.allocation,
                    ordinal: page.ordinal,
                    receipt,
                    freed: false,
                    refunded: false,
                });
            }
            Event::PaymentReleased {
                allocation,
                receipt,
                original_funder,
                process_only,
                bytes,
                ..
            } => {
                assert!(receipt != 0 && bytes > 0);
                // A transferred source may settle before the backing dies;
                // the replacement genuine payer must already cover it.
                assert!(
                    !self.pages.values().any(|page| !page.freed
                        && (page.payment.source_receipt == Some(receipt)
                            || page.payment.source_receipt.is_none()
                                && page.payment.fixed_cache_receipt == Some(receipt))),
                    "a current genuine payer cannot refund while its physical backing is still live"
                );
                if let Some((owner, held, released)) = self.reserves.get_mut(&receipt) {
                    assert!(!*released);
                    assert_eq!(original_funder, Some(*owner));
                    assert_eq!(
                        process_only, self.reserve_process_only[&receipt],
                        "the actual process-only/writer funding qualification survives final settlement"
                    );
                    assert_eq!(
                        *held, bytes,
                        "the original reserve refunds exactly its admitted bytes"
                    );
                    *released = true;
                } else {
                    let (owner, held, released) = self
                        .sources
                        .get_mut(&receipt)
                        .expect("refund joins its genuine original source admission");
                    assert!(!*released);
                    assert_eq!(original_funder, Some(*owner));
                    assert_eq!(*held, bytes);
                    *released = true;
                    assert!(self.pages.contains_key(&allocation));
                    self.source_refunds += 1;
                    if let Some(failure) = self
                        .failed_reads
                        .iter_mut()
                        .find(|failure| failure.receipt == receipt && !failure.refunded)
                    {
                        assert_eq!(failure.allocation, allocation);
                        assert!(
                            failure.freed,
                            "failed backing frees physically before its source refund"
                        );
                        failure.refunded = true;
                    }
                }
            }
            Event::OccupancyReleased {
                allocation,
                fixed_cache_receipt,
                fixed,
                bytes,
                ..
            } => {
                assert!(bytes > 0);
                assert!(self.pages.contains_key(&allocation));
                if fixed {
                    assert!(fixed_cache_receipt.is_some());
                }
                // A tally's release is deliberately not counted as a funder refund.
                self.occupancy_releases += 1;
            }
        }
    }
    pub fn assert_all_released(&self) {
        assert!(
            self.allocations > 0,
            "a zero-event capture proves no native page lifecycle"
        );
        assert_eq!(self.allocations, self.physical_frees);
        assert!(self.pages.values().all(|p| p.freed));
        assert!(self.reserves.values().all(|(_, _, released)| *released));
        assert!(self.sources.values().all(|(_, _, released)| *released));
        assert_eq!(self.unnamed_bytes, 0);
    }

    pub fn failed_read_count(&self) -> usize {
        self.failed_reads.len()
    }

    pub fn assert_failed_reads_released(&self) {
        assert!(
            !self.failed_reads.is_empty(),
            "the actual cold-error producer event must be reached"
        );
        assert!(
            self.failed_reads
                .iter()
                .all(|failure| failure.freed && failure.refunded),
            "each failed backing and its original invoice release before the failed query returns; valid metadata cache entries are separate"
        );
    }
}
