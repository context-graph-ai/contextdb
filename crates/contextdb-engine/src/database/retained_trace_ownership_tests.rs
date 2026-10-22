//! Policy 21: diagnostic allocations retain their actual executing reader until
//! native free. These rows use public execute and the real database trace slot.
//! No reset is part of ordinary execute; explicit clear below is a named phase.
use super::*;
use crate::persistence::content_identity::proof_allocation_oracle_tests as oracle;
use crate::vector_read_memory;
// Keep every mapped row on the original observation surface and require the
// complete parent ledger at each return, including refused/no-child/drop phases.
mod statement {
    pub(super) use crate::vector_read_memory::statement_child_proof::{
        ControlAllocation, NativeOwnerControls, Observation, native_controls,
    };
    pub(super) fn observe<T>(
        parent: &crate::vector_read_memory::DeclaredReadMemory,
        limit: u64,
        operation: impl FnOnce() -> T,
    ) -> (T, Observation) {
        let observed =
            crate::vector_read_memory::statement_child_proof::observe(parent, limit, operation);
        super::assert_complete_parent(&observed.1);
        observed
    }
}
use contextdb_core::read_memory::{ReadMemoryBudget, ReadMemoryOwner};
use contextdb_vector::mem::TraceInvoiceForTest;
use std::cell::{Cell, RefCell};
use std::sync::atomic::AtomicU64;

// capture969 begin
// Fixed passive sidecar for only the two existing failure controls. Sequence is
// thread-local program order, not a clock. No engine-stage formatting/allocation.
#[derive(Clone, Copy)]
struct OrderedCut {
    site: &'static str,
    held: usize,
    data: [usize; 8],
}
const EMPTY_CUT: OrderedCut = OrderedCut {
    site: "",
    held: 0,
    data: [0; 8],
};
struct OrderedCuts {
    active: bool,
    payer: usize,
    counter: usize,
    token: usize,
    count: usize,
    cuts: [OrderedCut; 512],
}
thread_local! {
    static ORDERED: RefCell<OrderedCuts> = const { RefCell::new(OrderedCuts {
        active: false, payer: 0, counter: 0, token: 0, count: 0, cuts: [EMPTY_CUT; 512],
    }) };
    static ORDERED_LOST: Cell<bool> = const { Cell::new(false) };
}
pub(crate) fn ordered_cut(site: &'static str, data: [usize; 8]) {
    ORDERED.with(|slot| {
        let Ok(mut cuts) = slot.try_borrow_mut() else {
            ORDERED_LOST.with(|lost| lost.set(true));
            return;
        };
        if !cuts.active {
            return;
        }
        if cuts.count == cuts.cuts.len() {
            ORDERED_LOST.with(|lost| lost.set(true));
            return;
        }
        // Active cuts occur while the real reader is borrowed by its producer,
        // token, or existing rollback pin. Disable before that pin can retire.
        let held = counter_value(cuts.counter);
        let index = cuts.count;
        cuts.cuts[index] = OrderedCut { site, held, data };
        cuts.count += 1;
    });
}
fn ordered_start(reader: &ReadMemoryOwner) {
    let payer = vector_read_memory::observed_reader_identity(reader);
    let counter = vector_read_memory::declared_child_observer(reader)
        .unwrap()
        .physical_funding_counter_for_test();
    ORDERED.with(|slot| {
        let mut cuts = slot.borrow_mut();
        cuts.active = true;
        cuts.payer = payer;
        cuts.counter = counter;
        cuts.count = 0;
        cuts.token = 0;
    });
    ORDERED_LOST.with(|lost| lost.set(false));
    contextdb_core::read_memory::set_trace_credit_drop_observer_for_test(
        reader.accounting_identity(),
        Some(ordered_credit_drop),
    );
    contextdb_vector::mem::set_trace_handoff_observer_for_test(Some(ordered_vector_cut));
    ordered_cut("original-child", [payer, counter, 0, 0, 0, 0, 0, 0]);
}
fn ordered_stop() {
    contextdb_core::read_memory::set_trace_credit_drop_observer_for_test(0, None);
    contextdb_vector::mem::set_trace_handoff_observer_for_test(None);
    ORDERED.with(|slot| slot.borrow_mut().active = false);
}
fn ordered_vector_cut(site: &'static str, address: usize, bytes: usize, payer: usize) {
    ordered_cut(site, [address, bytes, payer, 0, 0, 0, 0, 0]);
}
fn ordered_credit_drop(after: bool, address: usize, payer: usize, bytes: usize) {
    let payer = vector_read_memory::observed_identity(payer);
    if !after {
        ORDERED.with(|slot| slot.borrow_mut().token = address);
    }
    ordered_cut(
        if after {
            "credit-drop-after"
        } else {
            "credit-drop-before"
        },
        [address, bytes, payer, 0, 0, 0, 0, 0],
    );
    if after {
        ORDERED.with(|slot| slot.borrow_mut().token = 0);
    }
}
pub(crate) fn ordered_child_release(after: bool, payer: usize, bytes: usize) {
    let selected = ORDERED.with(|slot| {
        let cuts = slot.borrow();
        (cuts.active && cuts.payer == payer, cuts.token)
    });
    if selected.0 {
        ordered_cut(
            if after {
                "child-release-after"
            } else {
                "child-release-before"
            },
            [selected.1, bytes, payer, 0, 0, 0, 0, 0],
        );
    }
}
pub(crate) struct OrderedCallback(bool);
impl OrderedCallback {
    pub(crate) fn new(bytes: usize, phase: &str, reason: &str) -> Self {
        let selected = phase == "vector_copy" && reason == "prepaid_control";
        if selected {
            let (_, token) = crate::executor::bounded::retained_trace_payload_layouts_for_test();
            ordered_cut(
                "box-callback-entry",
                [0, bytes, token.align(), 0, 0, 0, 0, 0],
            );
        }
        Self(selected)
    }
}
impl Drop for OrderedCallback {
    fn drop(&mut self) {
        if self.0 {
            ordered_cut(
                if std::thread::panicking() {
                    "box-callback-unwind"
                } else {
                    "box-callback-return"
                },
                [0; 8],
            );
        }
    }
}
fn ordered_pressure(site: &'static str) {
    let parent = PRESSURE_PARENT.with(|slot| {
        slot.borrow()
            .as_ref()
            .map_or(0, |parent| parent.retained_bytes_for_test() as usize)
    });
    let bytes =
        PRESSURE_CREDIT.with(|slot| slot.borrow().as_ref().map_or(0, |credit| credit.bytes()));
    ordered_cut(site, [parent, bytes, 0, 0, 0, 0, 0, 0]);
}
// Called only after the query/exception has returned to the test harness.
// It prints every watch slot before ANY post-query assertion can hide evidence.
fn dump_ordered_cuts(label: &str) {
    let lost = ORDERED_LOST.with(Cell::get);
    ORDERED.with(|slot| {
        let cuts = slot.borrow();
        eprintln!(
            "capture969 {label} status={} payer={} counter={} cuts={}",
            if lost {
                "UNQUALIFIED_OVERFLOW_OR_REENTRANCY"
            } else {
                "CAPTURE_ONLY_UNQUALIFIED"
            },
            cuts.payer,
            cuts.counter,
            cuts.count
        );
        for (index, cut) in cuts.cuts[..cuts.count].iter().enumerate() {
            eprintln!(
                "capture969 seq={} site={} held={} data={:?}",
                index + 1,
                cut.site,
                cut.held,
                cut.data
            );
        }
    });
    let pending = ROLLBACK_TRACE.with(Cell::get);
    let factory = OWNER.with(Cell::get);
    let handoff = HANDOFF.with(Cell::get);
    eprintln!("capture969 pending={pending:?}");
    for (name, request) in [("factory", factory), ("handoff", handoff)] {
        eprintln!(
            "capture969 {} payer={} counter={} before={} after={} count={} allocations={:?} credits={:?}",
            name,
            request.payer,
            request.counter,
            request.before,
            request.after,
            request.count,
            &request.allocations[..request.count],
            &request.credits[..request.count]
        );
    }
    if let Some(pending) = pending {
        let count = pending
            .allocations
            .iter()
            .filter(|allocation| allocation.1 != 0)
            .count()
            + 2;
        let freed = oracle::source_free_for_test(count);
        eprintln!(
            "capture969 full-free-vector count={count} slots={:?}",
            &freed[..count]
        );
    }
}
// capture969 end
type Allocation = (usize, usize, usize);
#[derive(Clone, Copy, Debug)]
struct OwnerRequest {
    allocations: [Allocation; 256],
    credits: [usize; 256],
    count: usize,
    counter: usize,
    before: usize,
    after: usize,
    payer: usize,
}
const EMPTY_OWNER: OwnerRequest = OwnerRequest {
    allocations: [(0, 0, 0); 256],
    credits: [0; 256],
    count: 0,
    counter: 0,
    before: 0,
    after: 0,
    payer: 0,
};
thread_local! {
    static REFUSE_BOX: Cell<bool> = const { Cell::new(false) };
    static PENDING: Cell<Option<TraceInvoiceForTest>> = const { Cell::new(None) };
    static PRESSURE_PARENT: RefCell<Option<vector_read_memory::DeclaredReadRoot>> = const { RefCell::new(None) };
    static PRESSURE_CREDIT: RefCell<Option<contextdb_core::read_memory::ReadCredit>> = const { RefCell::new(None) };
    static NULL_KEEPER: Cell<bool> = const { Cell::new(false) };
    static ROLLBACK_PIN: RefCell<Option<OriginalPin>> = const { RefCell::new(None) };
    static ROLLBACK_TRACE: Cell<Option<TraceInvoiceForTest>> = const { Cell::new(None) };
    static ENABLED: Cell<bool> = const { Cell::new(false) };
    static OWNER: Cell<OwnerRequest> = const { Cell::new(EMPTY_OWNER) };
    static HANDOFF: Cell<OwnerRequest> = const { Cell::new(EMPTY_OWNER) };
}
pub(crate) struct OwnerWindow {
    window: oracle::SourceRequestsWindow,
    // Keep the actual original payer alive, including constructor refusal/unwind.
    reader: ReadMemoryOwner,
    counter: usize,
    before: usize,
    handoff: bool,
}
fn counter_value(counter: usize) -> usize {
    assert_ne!(
        counter, 0,
        "missing original-payer counter invalidates the proof"
    );
    // The caller pins the genuine original child, or borrows its live producer.
    unsafe { &*(counter as *const AtomicU64) }.load(Ordering::SeqCst) as usize
}
pub(crate) fn owner_window(
    reader: &ReadMemoryOwner,
    pending: TraceInvoiceForTest,
) -> Option<OwnerWindow> {
    // capture969 begin
    if ENABLED.with(Cell::get) && (REFUSE_BOX.with(Cell::get) || UNWIND_PROVIDER.with(Cell::get)) {
        ordered_start(reader);
    }
    // capture969 end
    if ENABLED.with(Cell::get) {
        PENDING.with(|slot| slot.set(Some(pending)));
    }
    if ENABLED.with(Cell::get) && NULL_KEEPER.with(|slot| slot.replace(false)) {
        // This is the real already-produced bounded trace, before the actual
        // factory. Pin its genuine payer; never construct a stand-in producer.
        ROLLBACK_PIN.with(|slot| {
            assert!(
                slot.borrow_mut()
                    .replace(OriginalPin::new(reader))
                    .is_none()
            )
        });
        ROLLBACK_TRACE.with(|slot| slot.set(Some(pending)));
        let counter = vector_read_memory::declared_child_observer(reader)
            .unwrap()
            .physical_funding_counter_for_test();
        let mut requests = [(0, 0, 0); 9];
        let mut count = 0;
        for allocation in pending.allocations {
            if allocation.1 != 0 {
                requests[count] = allocation;
                count += 1;
            }
        }
        assert!(
            count > 0,
            "a rollback proof requires real payload allocations"
        );
        watch_rollback_native(&mut requests, count, reader, counter);
        vector_read_memory::native_ownership_proof::arm_refuse_one_live_layout(
            contextdb_core::read_memory::ReadShared::<contextdb_core::read_memory::ReadCredit>::layout());
    }
    request_window(reader, false)
}
pub(crate) fn handoff_window(reader: &ReadMemoryOwner) -> Option<OwnerWindow> {
    if ENABLED.with(Cell::get) && REFUSE_BOX.with(|slot| slot.replace(false)) {
        let pending = PENDING
            .with(Cell::get)
            .expect("real bounded trace before handoff");
        ROLLBACK_TRACE.with(|slot| slot.set(Some(pending)));
        ROLLBACK_PIN.with(|slot| {
            assert!(
                slot.borrow_mut()
                    .replace(OriginalPin::new(reader))
                    .is_none()
            )
        });
        let counter = vector_read_memory::declared_child_observer(reader)
            .unwrap()
            .physical_funding_counter_for_test();
        let original_before = counter_value(counter);
        // Pressure the GENUINE parent, without changing its limit or funding
        // any byte of the target child. This separate debit only denies room.
        PRESSURE_PARENT.with(|slot| {
            let parent = slot.borrow();
            let parent = parent.as_ref().expect("explicit pressure parent");
            let remaining = parent
                .effective_limit_for_physical_test()
                .checked_sub(parent.retained_bytes_for_test())
                .unwrap();
            assert!(remaining > 0);
            let credit = contextdb_core::read_memory::ReadCredit::try_new(
                parent.original_owner(),
                usize::try_from(remaining).unwrap(),
                "reader_test",
                "pressure",
                "Release the deliberate parent pressure.",
            )
            .unwrap();
            PRESSURE_CREDIT.with(|slot| assert!(slot.borrow_mut().replace(credit).is_none()));
            assert_eq!(
                parent.retained_bytes_for_test(),
                parent.effective_limit_for_physical_test()
            );
        });
        assert_eq!(
            counter_value(counter),
            original_before,
            "parent pressure must NEVER supply any target-child credit"
        );
        let factory = OWNER.with(Cell::get);
        assert_eq!(factory.count, 2);
        let mut requests = [(0, 0, 0); 9];
        let mut count = 0;
        for allocation in pending.allocations {
            if allocation.1 != 0 {
                requests[count] = allocation;
                count += 1;
            }
        }
        // The independent post-call containment proof checks these identities
        // and K-before-A order against the genuine factory receipt.
        requests[count] = factory.allocations[1];
        count += 1;
        requests[count] = factory.allocations[0];
        count += 1;
        watch_rollback_native(&mut requests, count, reader, counter);
    }
    request_window(reader, true)
}
fn request_window(reader: &ReadMemoryOwner, handoff: bool) -> Option<OwnerWindow> {
    if !ENABLED.with(Cell::get) {
        return None;
    }
    // The unchanged native oracle samples AtomicUsize. Require the existing
    // reader-counter platform contract instead of truncating or subtracting it.
    assert_eq!(std::mem::size_of::<usize>(), std::mem::size_of::<u64>());
    assert_eq!(
        std::mem::align_of::<std::sync::atomic::AtomicUsize>(),
        std::mem::align_of::<AtomicU64>()
    );
    let counter = vector_read_memory::declared_child_observer(reader)
        .unwrap()
        .physical_funding_counter_for_test();
    // capture969 begin
    if handoff {
        ordered_cut("handoff-entry", [counter, 0, 0, 0, 0, 0, 0, 0]);
    }
    // capture969 end
    let before = counter_value(counter);
    Some(OwnerWindow {
        window: oracle::SourceRequestsWindow::begin(counter),
        reader: reader
            .original_native_owner_for_test()
            .expect("counter window pins only the original native child")
            .clone(),
        counter,
        before,
        handoff,
    })
}
impl Drop for OwnerWindow {
    fn drop(&mut self) {
        let (allocations, credits, count) = self.window.receipt();
        let request = OwnerRequest {
            allocations,
            credits,
            count,
            counter: self.counter,
            before: self.before,
            after: counter_value(self.counter),
            payer: vector_read_memory::observed_reader_identity(&self.reader),
        };
        // capture969 begin
        if self.handoff {
            ordered_cut(
                "handoff-window-drop",
                [self.counter, self.before, request.after, 0, 0, 0, 0, 0],
            );
        }
        // capture969 end
        if self.handoff {
            HANDOFF.with(|slot| slot.set(request));
        } else {
            OWNER.with(|slot| slot.set(request));
        }
    }
}
pub(crate) fn finish_owner_window(window: Option<OwnerWindow>) {
    drop(window);
}
struct Capture;
impl Capture {
    fn begin() -> Self {
        assert!(!ENABLED.with(|slot| slot.replace(true)));
        OWNER.with(|slot| slot.set(EMPTY_OWNER));
        HANDOFF.with(|slot| slot.set(EMPTY_OWNER));
        Self
    }
}
impl Drop for Capture {
    fn drop(&mut self) {
        // capture969 begin
        // An unexpected pre-handoff exit may never have installed the pin.
        if ROLLBACK_PIN.with(|slot| slot.borrow().is_none()) {
            ordered_stop();
        }
        // capture969 end
        ENABLED.with(|slot| slot.set(false));
        oracle::disarm_source_free_watch();
        NULL_KEEPER.with(|slot| slot.set(false));
        REFUSE_BOX.with(|slot| slot.set(false));
        PENDING.with(|slot| slot.set(None));
        // capture969 begin
        ordered_pressure("capture-pressure-drop-before");
        // capture969 end
        PRESSURE_CREDIT.with(|slot| drop(slot.borrow_mut().take()));
        // capture969 begin
        ordered_pressure("capture-pressure-drop-after");
        if ORDERED.with(|slot| slot.borrow().active) {
            dump_ordered_cuts("capture-cleanup-before-pin-retirement");
        }
        // capture969 end
        PRESSURE_PARENT.with(|slot| drop(slot.borrow_mut().take()));
        // Disarm a not-yet-consumed selected-layout fault on assertion unwind.
        let _ = vector_read_memory::native_ownership_proof::refuse_selected_live_layout(
            contextdb_core::read_memory::ReadShared::<contextdb_core::read_memory::ReadCredit>::layout());
        // capture969 begin
        ordered_stop();
        // capture969 end
        ROLLBACK_PIN.with(|slot| drop(slot.borrow_mut().take()));
        ROLLBACK_TRACE.with(|slot| slot.set(None));
        ROLLBACK_NATIVE_FREE.with(|slot| slot.set(None));
    }
}
fn invoice(db: &Database) -> TraceInvoiceForTest {
    db.last_vector_search_trace
        .read()
        .as_ref()
        .unwrap()
        .complete_invoice_for_test(|_| {})
}
#[derive(Clone, Copy, Debug)]
struct Controls {
    provider: Allocation,
    keeper: Allocation,
    native: statement::NativeOwnerControls,
    native_adapter: Allocation,
    native_keeper: Allocation,
}
impl Controls {
    fn bytes(self) -> usize {
        self.provider.1 + self.keeper.1
    }
    fn native_bytes(self) -> usize {
        self.native_adapter.1 + self.native_keeper.1
    }
    fn debit(self, receipt: TraceInvoiceForTest) -> usize {
        // The receipt names P/B/additional/score; both provider layers
        // contribute their separate Arc and keeper requests exactly once.
        receipt.prepaid_bytes
            + receipt.additional_bytes
            + receipt.score_credit_bytes
            + receipt.prepaid_control_bytes
            + self.bytes()
            + self.native_bytes()
    }
}
fn assert_owner_request(
    owner: OwnerRequest,
    receipt: TraceInvoiceForTest,
    observed: &statement::Observation,
) -> Controls {
    assert_rollback_native_frees();
    let proof = observed
        .trace_owner_controls
        .expect("real provider factory must be observed");
    assert_eq!(
        owner.count, 2,
        "record BOTH native requests, without truncation: {owner:?}"
    );
    assert_eq!(owner.payer, receipt.original_payer);
    assert_eq!(proof.payer, owner.payer);
    assert_eq!(proof.counter, owner.counter);
    assert_eq!(proof.provider_payload, receipt.owner_payload_pointer);
    assert_eq!(proof.before_admission as usize, owner.before);
    assert_eq!(proof.after_admission as usize, owner.after);
    let (payload, token) = crate::executor::bounded::retained_trace_payload_layouts_for_test();
    let provider_layout = std::alloc::Layout::new::<[std::sync::atomic::AtomicUsize; 2]>()
        .extend(payload)
        .unwrap()
        .0
        .pad_to_align();
    let keeper_layout = contextdb_core::read_memory::ReadShared::<
        contextdb_core::read_memory::ReadCredit,
    >::layout();
    assert_eq!(
        proof.provider_layout,
        (provider_layout.size(), provider_layout.align())
    );
    assert_eq!(
        proof.keeper_layout,
        (keeper_layout.size(), keeper_layout.align())
    );
    assert_eq!(
        (receipt.prepaid_control.1, receipt.prepaid_control.2),
        (token.size(), token.align()),
        "actual token Box uses its full concrete layout"
    );
    fn match_payload(
        owner: &OwnerRequest,
        payload: usize,
        expected: std::alloc::Layout,
    ) -> (usize, Allocation) {
        assert_ne!(payload, 0);
        let mut found = None;
        for (index, request) in owner.allocations[..owner.count].iter().copied().enumerate() {
            if request.0 <= payload && payload < request.0.checked_add(request.1).unwrap() {
                assert!(
                    found.is_none(),
                    "ambiguous containment invalidates the native proof"
                );
                assert_ne!(request.0, 0);
                assert_eq!((request.1, request.2), (expected.size(), expected.align()));
                assert_eq!(request.0 % request.2, 0);
                found = Some((index, request));
            }
        }
        found.expect("payload identity must match an actual measured request")
    }
    let (a_index, provider) = match_payload(&owner, proof.provider_payload, provider_layout);
    let (k_index, keeper) = match_payload(&owner, proof.keeper_payload, keeper_layout);
    assert_ne!(
        a_index, k_index,
        "provider and keeper are separate physical allocations"
    );
    assert!(
        k_index < a_index,
        "the real keeper is allocated before the provider Arc"
    );
    let native = observed
        .native_controls
        .expect("original native controls must be observed");
    let (native_adapter, native_keeper) = assert_native_requests(native, observed);
    assert_eq!(proof.parent, native.parent);
    assert_eq!(proof.parent_counter, native.parent_counter);
    assert_eq!(proof.native_forwarder, native.adapter.unwrap().payload);
    assert_eq!(proof.native_keeper, native.keeper.unwrap().payload);
    assert_eq!(proof.native_adapter_invoice_bytes, native_adapter.1);
    let controls = Controls {
        provider,
        keeper,
        native,
        native_adapter,
        native_keeper,
    };
    let admitted = owner.before.checked_add(controls.bytes()).unwrap();
    assert_eq!(
        owner.after, admitted,
        "charge each original-payer control exactly once"
    );
    for index in [k_index, a_index] {
        assert_eq!(
            owner.credits[index], admitted,
            "BOTH controls must be admitted to the original child before either native request"
        );
    }
    controls
}
fn assert_box_request(receipt: TraceInvoiceForTest) {
    let actual = HANDOFF.with(Cell::get);
    assert_eq!(
        actual.count, 1,
        "the real handoff must record its actual token Box request"
    );
    assert_eq!(actual.allocations[0], receipt.prepaid_control);
    assert_eq!(actual.payer, receipt.original_payer);
    assert_eq!(actual.after, actual.before + receipt.prepaid_control_bytes);
    assert_eq!(
        actual.credits[0], actual.after,
        "the original child admits the actual Box before the native allocator request"
    );
}
fn assert_invoice(
    receipt: TraceInvoiceForTest,
    reader: &statement::Observation,
    controls: Controls,
) {
    assert_box_request(receipt);
    assert_eq!(receipt.original_payer, reader.owner_identity);
    assert_eq!(
        receipt.forwarding_payer, reader.owner_identity,
        "prepaid payload and control refunds must return to the SAME original reader"
    );
    assert_eq!(
        receipt.payload_layout_bytes(),
        receipt.prepaid_bytes,
        "actual buffer capacities reconcile the complete payload, without a magic residual"
    );
    assert_eq!(receipt.prepaid_control.1, receipt.prepaid_control_bytes);
    let mut physical = [(0, 0, 0); 9];
    let mut count = 0;
    for request in receipt.allocations {
        if request.1 != 0 {
            assert_ne!(request.0, 0);
            assert!(
                physical[..count].iter().all(|old| old.0 != request.0),
                "nonempty physical payload requests must be distinct"
            );
            physical[count] = request;
            count += 1;
        }
    }
    for request in [
        receipt.prepaid_control,
        controls.provider,
        controls.keeper,
        controls.native_adapter,
        controls.native_keeper,
        actual_control(controls.native.block),
    ] {
        assert_ne!(request.0, 0);
        assert!(
            physical[..count].iter().all(|old| old.0 != request.0),
            "each actual request enters the complete physical invoice exactly once"
        );
        physical[count] = request;
        count += 1;
    }
    assert_eq!(
        count,
        receipt.allocations.iter().filter(|p| p.1 != 0).count() + 6
    );
    assert_eq!(
        controls.debit(receipt) as u64,
        reader.identity.target_last_held,
        "P + B + At + Kt + An + Kn + additional + score equals the actual original-child working debit"
    );
    assert_eq!(
        reader.identity.target_reserves - reader.identity.target_releases,
        controls.debit(receipt) as u64
    );
    assert!(!reader.identity.target_drop_seen);
    assert_eq!(reader.identity.target_identity, receipt.original_payer);
    assert_eq!(reader.children, 1);
    assert!(receipt.owner_control_invoice_present);
    assert!(receipt.admission_control_invoice_present);
    assert!(receipt.prepaid_control_bytes > 0);
    assert!(
        receipt.prepaid_bytes > 0,
        "native payload funding proof must not be vacuous"
    );
    let original =
        vector_read_memory::original_child_observer_for_test(receipt.original_payer).unwrap();
    let pin = original.retain_original_for_physical_test().unwrap();
    assert_eq!(
        counter_value(original.physical_funding_counter_for_test()),
        controls.debit(receipt)
    );
    assert_eq!(
        original.sample().charged_bytes,
        controls.debit(receipt) as u64
    );
    drop(pin);
}
// A measured request is mandatory. Expected Layout metadata cannot qualify an
// unarmed meter, and an Arc handle's size cannot price its allocation.
fn actual_control(control: statement::ControlAllocation) -> Allocation {
    let request = control
        .actual_request
        .expect("UNKNOWN native request cannot qualify physical funding");
    assert_ne!(request.0, 0);
    assert_ne!(control.payload, 0);
    assert_eq!((request.1, request.2), control.layout);
    assert_eq!(request.0 % request.2, 0);
    assert!(
        request.0 <= control.payload && control.payload < request.0.checked_add(request.1).unwrap()
    );
    assert!(control.ordinal > 0);
    request
}
fn native_requests(native: statement::NativeOwnerControls) -> (Allocation, Allocation) {
    let block = actual_control(native.block);
    let expected = vector_read_memory::native_control_layouts_for_test().1;
    assert_eq!(
        (block.1, block.2),
        (expected.size(), expected.align()),
        "actual original block must match the declared native child/invoice type"
    );
    let keeper = native.keeper.expect("genuine native keeper");
    let adapter = native.adapter.expect("genuine native adapter");
    let k = actual_control(keeper);
    let a = actual_control(adapter);
    let inventory = crate::executor::bounded::ordinary_native_control_layouts_for_test();
    assert_eq!((k.1, k.2), (inventory[1].size(), inventory[1].align()));
    assert_eq!((a.1, a.2), (inventory[2].size(), inventory[2].align()));
    assert_eq!(native.owner, block.0);
    for address in [native.payload, native.counter] {
        assert!(block.0 <= address && address < block.0.checked_add(block.1).unwrap());
    }
    assert_eq!(native.keeper_payer, Some(native.owner));
    assert_eq!(native.adapter_payer, Some(native.owner));
    assert_eq!(native.keeper_credit_bytes, k.1);
    assert_eq!(native.adapter_invoice_bytes, a.1);
    assert_eq!(native.outer_keeper, keeper.payload);
    assert_eq!(
        native.inner_keeper, keeper.payload,
        "both aliases retain one original keeper; they are not two charges"
    );
    let k_layout =
        contextdb_core::read_memory::ReadShared::<contextdb_core::read_memory::ReadCredit>::layout(
        );
    assert_eq!((k.1, k.2), (k_layout.size(), k_layout.align()));
    let payload = std::alloc::Layout::from_size_align(
        native.adapter_payload_layout.0,
        native.adapter_payload_layout.1,
    )
    .unwrap();
    let a_layout = std::alloc::Layout::new::<[std::sync::atomic::AtomicUsize; 2]>()
        .extend(payload)
        .unwrap()
        .0
        .pad_to_align();
    assert_eq!(
        (a.1, a.2),
        (a_layout.size(), a_layout.align()),
        "concrete Arc layout expectation must equal the actual request"
    );
    assert!(native.block.ordinal < keeper.ordinal && keeper.ordinal < adapter.ordinal);
    assert_ne!(block.0, a.0);
    assert_ne!(block.0, k.0);
    assert_ne!(a.0, k.0);
    assert!(!native.keeper_refused && !native.adapter_refused);
    assert_eq!(native.initial_child_bytes, 0);
    assert_eq!(
        native.parent_after_block,
        native
            .parent_before_block
            .expect("original parent admission")
            + block.1 as u64
    );
    let kb = native
        .keeper_before
        .expect("original keeper admission entry");
    let ka = native
        .keeper_after
        .expect("original keeper admission completion");
    let ab = native
        .adapter_before
        .expect("original adapter admission entry");
    let aa = native
        .adapter_after
        .expect("original adapter admission completion");
    for sample in [kb, ka, ab, aa] {
        assert_eq!(sample.child_counter, native.counter);
        assert_eq!(sample.parent_counter, native.parent_counter);
    }
    assert_eq!(kb.child_bytes, 0);
    assert_eq!(ka.child_bytes, k.1 as u64);
    assert_eq!(ab.child_bytes, ka.child_bytes);
    assert_eq!(aa.child_bytes, (a.1 + k.1) as u64);
    assert_eq!(ka.parent_bytes, kb.parent_bytes + k.1 as u64);
    assert_eq!(ab.parent_bytes, ka.parent_bytes);
    assert_eq!(aa.parent_bytes, ab.parent_bytes + a.1 as u64);
    (a, k)
}
fn assert_complete_parent(observed: &statement::Observation) {
    let identity = observed.identity;
    assert_eq!(observed.parent_funding_mismatches, 0, "{observed:?}");
    assert_eq!(observed.complete_parent_mismatches, 0, "{observed:?}");
    let inline_delta = identity.target_last_held as i128 - identity.target_initial_held as i128;
    assert_eq!(
        identity.target_parent_net, inline_delta,
        "compare forwarded debit against the independently sampled inline counter"
    );
    let expected = observed.parent_before as i128
        + inline_delta
        + identity.sibling_parent_net
        + identity.direct_parent_net
        + identity.target_control_parent_net;
    assert_eq!(
        observed.parent_after as i128, expected,
        "original parent conservation includes actual sibling and direct control refunds"
    );
    if observed.owner_identity != 0 {
        let equation = observed.parent_equation;
        assert_eq!(equation.target_current, identity.target_last_held);
        assert_eq!(equation.target_initial, identity.target_initial_held);
        assert_eq!(equation.target_forwarded_net, inline_delta);
        assert_eq!(equation.baseline, observed.parent_before);
        assert_eq!(equation.sibling_net, identity.sibling_parent_net);
        assert_eq!(equation.direct_net, identity.direct_parent_net);
        assert_eq!(
            equation.target_control_net,
            identity.target_control_parent_net
        );
        assert_eq!(equation.expected, expected);
        assert_eq!(equation.actual as i128, expected);
        assert_eq!(observed.complete_parent_expected, expected);
        assert_eq!(observed.complete_parent_actual, observed.parent_after);
    }
    // The rejected one-child hypothesis stays in the receipt, never waived or
    // substituted for the complete ledger's required zero mismatches.
    let _rejected_one_child_hypothesis = observed.single_child_parent_mismatches;
}
fn assert_native_requests(
    native: statement::NativeOwnerControls,
    observed: &statement::Observation,
) -> (Allocation, Allocation) {
    assert_eq!(native.owner, observed.owner_identity);
    assert_eq!(native.parent, observed.parent_identity);
    assert_eq!(native.counter, observed.native_counter);
    assert_eq!(native.payload, observed.native_payload_identity);
    assert_eq!(native.block.layout.0, observed.control_bytes);
    let requests = native_requests(native);
    let adapter = native.adapter.expect("original native adapter constructor");
    let history = observed
        .native_forwarder_history_not_live
        .expect("original constructor history is mandatory, even after retirement");
    assert_eq!(
        actual_control(history),
        requests.0,
        "history must contain the actual original adapter request, not just its expected Layout"
    );
    assert_eq!(history.payload, adapter.payload);
    assert_eq!(history.layout, adapter.layout);
    assert_eq!(
        history.ordinal, adapter.ordinal,
        "a reused address cannot substitute for the original constructor attempt"
    );
    // The rollback watch was armed on these live originals before the query
    // returned/unwound. Retain its scalar identity until this post-query check;
    // historical payloads are never promoted, dereferenced, or treated as pins.
    if let Some((_, watched)) = ROLLBACK_NATIVE_FREE.with(Cell::get) {
        assert_eq!(
            (
                watched.owner,
                watched.counter,
                watched.parent,
                watched.parent_counter
            ),
            (
                native.owner,
                native.counter,
                native.parent,
                native.parent_counter
            )
        );
        for (original, selected) in [
            (watched.block, native.block),
            (watched.adapter.unwrap(), adapter),
            (watched.keeper.unwrap(), native.keeper.unwrap()),
        ] {
            assert_eq!(actual_control(original), actual_control(selected));
            assert_eq!(original.payload, selected.payload);
            assert_eq!(original.layout, selected.layout);
            assert_eq!(original.ordinal, selected.ordinal);
        }
        assert_rollback_native_frees();
        assert_eq!(
            observed.native_forwarder_identity, 0,
            "the physically freed original adapter must have retired its live alias"
        );
        ROLLBACK_NATIVE_FREE.with(|slot| slot.set(None));
    } else {
        assert_eq!(
            observed.native_forwarder_identity, adapter.payload,
            "without original physical-free evidence the live alias must still name the constructor"
        );
    }
    requests
}

// This is the genuine in-block owner, not the forwarding Arc. Keeping it until
// the end lets An/Kn physically free while the inline counter is still live.
// Database remains alive at every use, including the parent sample AFTER N free.
struct OriginalPin {
    owner: Option<contextdb_core::read_memory::ReadMemoryOwner>,
    native: statement::NativeOwnerControls,
}
impl OriginalPin {
    fn new(reader: &ReadMemoryOwner) -> Self {
        let reader = reader
            .original_native_owner_for_test()
            .expect("borrow the genuine native child through its actual forwarder");
        let native =
            statement::native_controls(vector_read_memory::observed_reader_identity(reader))
                .expect("pin the actual selected native child");
        assert!(reader.is_native());
        assert_eq!(reader.accounting_identity(), native.owner);
        native_requests(native);
        Self {
            owner: Some(reader.clone()),
            native,
        }
    }
    fn from_observation(
        db: &Database,
        original: &vector_read_memory::DeclaredReadChildObserver,
        native: statement::NativeOwnerControls,
    ) -> Self {
        native_requests(native);
        let mut owner = None;
        db.retained_vector_trace_invoice_for_test(|reader| {
            assert!(reader.is_native());
            assert_eq!(reader.accounting_identity(), native.owner);
            assert_eq!(
                original.sample().owner_identity,
                reader.accounting_identity()
            );
            owner = Some(reader.clone());
        })
        .expect("borrow the actual retained trace payer");
        Self {
            owner: Some(owner.expect("retained trace must expose its original payer")),
            native,
        }
    }
}
impl Drop for OriginalPin {
    fn drop(&mut self) {
        ordered_stop();
        // The counter lives in N. Disarm EVERY child-counter watch before N
        // can die; the final free uses only the original parent's counter.
        oracle::disarm_source_free_watch();
        if std::thread::panicking() {
            // The test is already failing; never sample a dying child or
            // replace that failure with a destructor's second panic.
            drop(self.owner.take());
            return;
        }
        let block = actual_control(self.native.block);
        assert_eq!(
            counter_value(self.native.counter),
            0,
            "all child-funded physical controls must retire before the final native pin"
        );
        let parent_before = counter_value(self.native.parent_counter);
        oracle::watch_exact_free_for_test(block, self.native.parent_counter);
        drop(self.owner.take());
        let free = oracle::source_free_for_test(1)[0];
        oracle::disarm_source_free_watch();
        assert_eq!(
            free,
            (parent_before, parent_before, 1),
            "N must physically free exactly once BEFORE its original parent credit returns"
        );
        assert_eq!(
            counter_value(self.native.parent_counter),
            parent_before - block.1,
            "N's original parent invoice returns exactly once after System.dealloc"
        );
    }
}

thread_local! {
    static ROLLBACK_NATIVE_FREE: Cell<Option<(usize, statement::NativeOwnerControls)>> = const { Cell::new(None) };
}
fn watch_rollback_native(
    requests: &mut [Allocation],
    count: usize,
    reader: &ReadMemoryOwner,
    counter: usize,
) {
    let native =
        statement::native_controls(vector_read_memory::observed_reader_identity(reader)).unwrap();
    let (a, k) = native_requests(native);
    assert_eq!(
        counter, native.counter,
        "the free watch must sample the actual original native payer"
    );
    requests[count] = a;
    requests[count + 1] = k;
    ROLLBACK_NATIVE_FREE.with(|slot| {
        assert!(slot.replace(Some((count, native))).is_none());
    });
    oracle::watch_exact_frees_for_test(&requests[..count + 2], counter);
}
fn assert_rollback_native_frees() {
    // Non-consuming: query's normal-return guard and the post-catch helper both
    // validate these same slots before identity qualification consumes the receipt.
    if let Some((offset, native)) = ROLLBACK_NATIVE_FREE.with(Cell::get) {
        let (a, k) = native_requests(native);
        let freed = oracle::source_free_for_test(offset + 2);
        assert_eq!(
            freed[offset],
            (a.1 + k.1, a.1 + k.1, 1),
            "actual native adapter must physically free before its original An credit returns"
        );
        assert_eq!(
            freed[offset + 1],
            (k.1, k.1, 1),
            "actual native keeper must physically free before its original Kn credit returns"
        );
    }
}

fn fixture() -> (tempfile::TempDir, Database) {
    let directory = tempfile::tempdir().unwrap();
    let db = Database::open_with_options(
        directory.path().join("retained-trace.db"),
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            memory_limit: Some(64 * 1024 * 1024),
            ..Default::default()
        },
    )
    .unwrap();
    db.execute(
        "CREATE TABLE unpartitioned_items (id INT PRIMARY KEY, v VECTOR(4)) HISTORY ALL",
        &HashMap::new(),
    )
    .unwrap();
    for id in 0..4 {
        db.execute(
            "INSERT INTO unpartitioned_items VALUES ($id,$v)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("v".into(), Value::Vector(vec![1.0, id as f32, 0.0, 0.0])),
            ]),
        )
        .unwrap();
    }
    (directory, db)
}
fn query(db: &Database) -> Result<QueryResult> {
    struct CheckNativeFrees;
    impl Drop for CheckNativeFrees {
        fn drop(&mut self) {
            // A provider fault is caught by the existing row. Validate its
            // untouched free slots in assert_owner_request after catch_unwind,
            // so a bad free becomes a finding rather than a double panic.
            if !std::thread::panicking() {
                assert_rollback_native_frees();
            }
        }
    }
    let check_native_frees = CheckNativeFrees;
    let (answer, _, _) = oracle::measured(|| {
        db.execute_at_snapshot(
            "SELECT id FROM unpartitioned_items ORDER BY v <=> $v USE VECTOR EXACT LIMIT 1",
            &HashMap::from([("v".into(), Value::Vector(vec![1.0, 0.0, 0.0, 0.0]))]),
            db.snapshot(),
        )
    });
    drop(check_native_frees);
    answer
}
#[derive(Clone, Copy)]
enum Row {
    Lifecycle,
    OwnerAdmission,
    OwnerNativeFree,
    Refusal,
    Unwind,
}
// This checks copies of actual allocator/invoice receipts. Negative controls
// below mutate those copies; they do not claim to mutate production refunds.
#[derive(Clone, Copy)]
struct RetainedFreeCut {
    request: Allocation,
    original_counter: usize,
    watched_counter: usize,
    original_payer: usize,
    observed_payer: usize,
    freed: (usize, usize, usize),
    live: usize,
    residual: usize,
}
fn retained_free_cut_matches(cut: RetainedFreeCut) -> bool {
    cut.request.0 != 0
        && cut.request.1 != 0
        && cut.request.2.is_power_of_two()
        && cut.original_counter != 0
        && cut.original_counter == cut.watched_counter
        && cut.original_payer != 0
        && cut.original_payer == cut.observed_payer
        && cut.freed.2 == 1
        && cut.freed.0 == cut.freed.1
        && cut.live >= cut.request.1
        && cut.live.checked_add(cut.residual) == Some(cut.freed.0)
}
fn retained_trace_row(row: Row) {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let (_directory, db) = fixture();
    let parent = db.declared_read_memory_for_test();
    let allowance = parent.effective_limit_for_physical_test();
    assert!(allowance > parent.retained_bytes_for_test());
    let writer_before = db.accountant().usage().used;
    let _capture = Capture::begin();
    let (_, first) = statement::observe(&parent, allowance, || {
        let answer = query(&db);
        assert_eq!(answer.unwrap().rows(), vec![vec![Value::Int64(0)]]);
    });
    let first_invoice = invoice(&db);
    let first_owner = OWNER.with(Cell::get);
    let controls = assert_owner_request(first_owner, first_invoice, &first);
    assert_invoice(first_invoice, &first, controls);
    assert_eq!(
        first.trace_produced.unwrap().allocations,
        first_invoice.allocations,
        "the slot owns the producer's actual buffers, without a diagnostic copy"
    );
    assert_eq!(first.trace_new, Some(first_invoice));
    assert_eq!(db.accountant().usage().used, writer_before);

    // All five controls share the complete admission AND physical-free proof;
    // no admission-only early return can leave keeper funding untested.
    if matches!(row, Row::Refusal) {
        let held = parent.retained_bytes_for_test();
        let (answer, refused) = statement::observe(&parent, held, || query(&db));
        assert!(matches!(answer, Err(Error::MemoryBudgetExceeded { .. })));
        assert_eq!(
            refused.children, 0,
            "zero headroom cannot admit even a child control"
        );
        assert_eq!(refused.trace_produced, None);
        assert_eq!(refused.trace_new, None);
        assert_eq!(
            invoice(&db),
            first_invoice,
            "refusal cannot replace the standing trace"
        );
        assert_eq!(parent.retained_bytes_for_test(), held);
        // Resume at the genuine parent ceiling and prove this SAME standing
        // trace's eventual frees, rather than ending at refusal preservation.
    }
    // Deliberate harness alias: pins only the original child so the existing
    // physical-free oracle may safely sample its counter through every free.
    // It neither pays nor clones the trace, and its release is a separate phase.
    let original =
        vector_read_memory::original_child_observer_for_test(first_invoice.original_payer).unwrap();
    let pin = OriginalPin::from_observation(&db, &original, controls.native);
    let counter = original.physical_funding_counter_for_test();
    let mut requests = [(0, 0, 0); 9];
    let mut count = 0;
    for request in first_invoice.allocations {
        if request.1 != 0 {
            requests[count] = request;
            count += 1;
        }
    }
    let payload_count = count;
    requests[count] = first_invoice.prepaid_control;
    count += 1;
    requests[count] = controls.provider;
    count += 1;
    requests[count] = controls.keeper;
    count += 1;
    requests[count] = controls.native_adapter;
    count += 1;
    requests[count] = controls.native_keeper;
    count += 1;
    assert_eq!(
        count,
        payload_count + 5,
        "every nonempty P plus B/At/Kt/An/Kn"
    );
    // Existing exact-free oracle uses AtomicUsize. This platform assertion is
    // required by its existing original-reader use, not a guessed Arc layout.
    assert_eq!(std::mem::size_of::<usize>(), std::mem::size_of::<u64>());
    oracle::watch_exact_frees_for_test(&requests[..count], counter);
    let (_, next) = statement::observe(&parent, allowance, || {
        if matches!(row, Row::Unwind) {
            // Existing real boundary: caller unwinds AFTER public return with
            // the legitimate slot retained. No production fault switch added.
            let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                assert_eq!(query(&db).unwrap().rows(), vec![vec![Value::Int64(0)]]);
                panic!("caller unwind after public return");
            }));
            assert!(caught.is_err());
        } else {
            assert_eq!(query(&db).unwrap().rows(), vec![vec![Value::Int64(0)]]);
        }
    });
    let next_invoice = invoice(&db);
    let next_owner = OWNER.with(Cell::get);
    let next_controls = assert_owner_request(next_owner, next_invoice, &next);
    assert_invoice(next_invoice, &next, next_controls);
    assert_ne!(first_invoice.original_payer, next_invoice.original_payer);
    assert_eq!(
        next.trace_cleared,
        Some(first_invoice),
        "the existing bounded search clears the old slot before producing the new trace"
    );
    assert_eq!(
        next.trace_old, None,
        "publication replaces the now-empty slot on this route"
    );
    assert_eq!(next.trace_new, Some(next_invoice));
    let freed = oracle::source_free_for_test(count);
    let live = oracle::source_live_at_free_for_test(count);
    for index in 0..count {
        assert_eq!(
            freed[index].2, 1,
            "each ORIGINAL allocation physically freed exactly once: {requests:?} {freed:?}"
        );
        if index < payload_count {
            // Payload buffers share one prepaid invoice. Their physical
            // frees precede that invoice's refund; keep the original exact
            // complete invoice assertion at every one of these cuts.
            assert_eq!(
                (freed[index].0, freed[index].1),
                (controls.debit(first_invoice), controls.debit(first_invoice))
            );
            continue;
        }
        // The score credit retires after the payload group, before B. Its
        // original assertion at B includes additional but not score bytes.
        let residual = if index == payload_count {
            first_invoice.additional_bytes
        } else {
            0
        };
        let cut = RetainedFreeCut {
            request: requests[index],
            original_counter: controls.native.counter,
            watched_counter: counter,
            original_payer: first_invoice.original_payer,
            observed_payer: first_owner.payer,
            freed: freed[index],
            live: live[index],
            residual,
        };
        assert!(
            retained_free_cut_matches(cut),
            "actual original payer must equal independently recorded live allocation requests plus its retained invoice: index={index} live={live:?} requests={requests:?} freed={freed:?}"
        );
        let mut bad = cut;
        bad.freed.2 = 0;
        assert!(
            !retained_free_cut_matches(bad),
            "missing physical free is rejected"
        );
        let mut bad = cut;
        bad.freed.2 = 2;
        assert!(
            !retained_free_cut_matches(bad),
            "duplicate physical free is rejected"
        );
        let mut bad = cut;
        bad.freed.0 -= cut.request.1;
        bad.freed.1 = bad.freed.0;
        assert!(!retained_free_cut_matches(bad), "early refund is rejected");
        let mut bad = cut;
        bad.observed_payer ^= 1;
        assert!(
            !retained_free_cut_matches(bad),
            "wrong original payer is rejected"
        );
        let mut bad = cut;
        bad.watched_counter ^= 1;
        assert!(
            !retained_free_cut_matches(bad),
            "wrong watched counter is rejected"
        );
        let mut bad = cut;
        bad.freed.0 += cut.request.1;
        bad.freed.1 = bad.freed.0;
        assert!(
            !retained_free_cut_matches(bad),
            "extra residual allocation is rejected"
        );
    }
    let token = payload_count;
    let provider = token + 1;
    let keeper = token + 2;
    let adapter = token + 3;
    let native_keeper = token + 4;
    // This is the same actual order as the protected physical-free contract:
    // payload group -> B -> An -> Kn -> At -> Kt. Each control's own Layout
    // is refunded only after its physical free; no control is pinned to force
    // the superseded hand-written At/Kt-before-An/Kn assumption.
    assert_eq!(
        freed[token].0,
        first_invoice.prepaid_control_bytes
            + first_invoice.additional_bytes
            + controls.bytes()
            + controls.native_bytes()
    );
    assert_eq!(
        freed[token].0 - freed[adapter].0,
        requests[token].1 + first_invoice.additional_bytes
    );
    for (earlier, later) in [
        (adapter, native_keeper),
        (native_keeper, provider),
        (provider, keeper),
    ] {
        assert!(live[earlier] > live[later]);
        assert_eq!(freed[earlier].0 - freed[later].0, requests[earlier].1);
    }
    assert_eq!(freed[keeper].0, requests[keeper].1);
    assert_eq!(
        counter_value(counter),
        0,
        "old trace returns its exact complete invoice"
    );
    assert!(
        original.sample().alive,
        "the deliberate original-child pin is still live"
    );
    oracle::disarm_source_free_watch();
    let (_, retired) = statement::observe(&parent, allowance, || drop(pin));
    assert_eq!(
        retired.drops, 1,
        "one original payload Drop after the last deliberate alias"
    );
    assert_eq!(
        retired.identity.last_sibling_drop.unwrap().child_identity,
        first_invoice.original_payer
    );
    assert_eq!(
        retired.parent_before - retired.parent_after,
        first.control_bytes as u64
    );
    assert!(
        !original.sample().alive,
        "last actual alias gone: original target retires"
    );
    // Explicit observed lifecycle phase, never ordinary execute behavior.
    let next_original =
        vector_read_memory::original_child_observer_for_test(next_invoice.original_payer).unwrap();
    let next_pin = OriginalPin::from_observation(&db, &next_original, next_controls.native);
    assert_eq!(
        next_original.sample().charged_bytes,
        next_controls.debit(next_invoice) as u64
    );
    db.clear_last_vector_search_trace();
    assert_eq!(next_original.sample().charged_bytes, 0);
    assert!(next_original.sample().alive);
    let (_, next_retired) = statement::observe(&parent, allowance, || drop(next_pin));
    assert_eq!(next_retired.drops, 1);
    assert_eq!(
        next_retired
            .identity
            .last_sibling_drop
            .unwrap()
            .child_identity,
        next_invoice.original_payer
    );
    assert!(!next_original.sample().alive);
    assert_eq!(parent.retained_bytes_for_test(), first.parent_before);
    assert_eq!(db.accountant().usage().used, writer_before);
}

#[test]
#[serial_test::serial]
fn retained_trace_payload_and_prepaid_box_keep_original_invoice_through_native_frees() {
    retained_trace_row(Row::Lifecycle);
}
#[test]
#[serial_test::serial]
fn retained_trace_actual_producer_prepays_its_owner_arc() {
    retained_trace_row(Row::OwnerAdmission);
}
#[test]
#[serial_test::serial]
fn retained_trace_zero_headroom_preserves_the_existing_slot() {
    retained_trace_row(Row::Refusal);
}
#[test]
#[serial_test::serial]
fn retained_trace_caller_unwind_preserves_the_same_original_lifetime() {
    retained_trace_row(Row::Unwind);
}

#[test]
#[serial_test::serial]
fn retained_trace_original_owner_arc_stays_paid_through_native_free() {
    retained_trace_row(Row::OwnerNativeFree);
}

// Policy 21 native-refusal row: a successful allocation control cannot prove
// rollback when the actual fallible keeper allocation returns NULL. The named
// decisive mutant keeps the provider admission after ReadShared returns NULL.
#[test]
#[serial_test::serial]
fn retained_trace_keeper_native_null_returns_controls_and_keeps_payload_paid_through_free() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let (_directory, db) = fixture();
    let parent = db.declared_read_memory_for_test();
    let allowance = parent.effective_limit_for_physical_test();
    let writer_before = db.accountant().usage().used;
    let _capture = Capture::begin();
    let (answer, first) = statement::observe(&parent, allowance, || query(&db));
    assert_eq!(answer.unwrap().rows(), vec![vec![Value::Int64(0)]]);
    let old = invoice(&db);
    let controls = assert_owner_request(OWNER.with(Cell::get), old, &first);
    assert_invoice(old, &first, controls);
    NULL_KEEPER.with(|slot| slot.set(true));
    let (answer, failed) = statement::observe(&parent, allowance, || query(&db));
    assert!(
        !NULL_KEEPER.with(Cell::get),
        "the actual trace factory must be reached"
    );
    assert!(
        !vector_read_memory::native_ownership_proof::refusal_armed_for_test(),
        "the real allocator must consume the keeper NULL injection"
    );
    assert!(
        matches!(answer, Err(Error::Other(ref message))
        if message.contains("cannot allocate admitted shared control")),
        "the native keeper NULL must surface its real refusal: {answer:?}"
    );
    assert_eq!(
        failed.trace_cleared,
        Some(old),
        "search entry already cleared the old slot before this later native refusal"
    );
    assert_eq!(failed.trace_new, None);
    assert!(db.last_vector_search_trace.read().is_none());
    let pending = ROLLBACK_TRACE
        .with(Cell::get)
        .expect("observe the real pending producer");
    assert_eq!(pending.payload_layout_bytes(), pending.prepaid_bytes);
    let factory = OWNER.with(Cell::get);
    assert_ne!(factory.counter, 0);
    assert_eq!(
        factory.count, 0,
        "keeper NULL must produce neither owner allocation"
    );
    assert!(
        failed.trace_owner_controls.is_none(),
        "a failed factory publishes no owner receipt"
    );
    assert_eq!(factory.payer, failed.owner_identity);
    assert_eq!(
        factory.after, factory.before,
        "both original A and K admissions return on the actual native NULL branch"
    );
    let original = vector_read_memory::original_child_observer_for_test(factory.payer).unwrap();
    assert!(
        original.sample().alive,
        "the deliberate rollback pin keeps the counter safe"
    );
    let count = pending
        .allocations
        .iter()
        .filter(|allocation| allocation.1 != 0)
        .count();
    let freed = oracle::source_free_for_test(count);
    for free in &freed[..count] {
        assert_eq!(
            free.2, 1,
            "each failed producer buffer must physically free once"
        );
        assert_eq!(
            (free.0, free.1),
            (factory.before, factory.before),
            "native NULL returns A/K first; the ENTIRE pending payload and standing request debit survive each actual buffer free: {freed:?}"
        );
    }
    assert_eq!(counter_value(factory.counter), 0);
    assert_eq!(original.sample().charged_bytes, 0);
    assert_eq!(
        failed.identity.target_reserves,
        failed.identity.target_releases
    );
    assert!(
        !failed.identity.target_drop_seen,
        "payload debit returns before releasing the explicit pin"
    );
    assert_eq!(
        failed.parent_after,
        first.parent_before + failed.control_bytes as u64
    );
    oracle::disarm_source_free_watch();
    let (_, retired) = statement::observe(&parent, allowance, || {
        // capture969 begin
        ordered_stop();
        // capture969 end
        ROLLBACK_PIN.with(|slot| drop(slot.borrow_mut().take()));
    });
    assert_eq!(retired.drops, 1);
    assert_eq!(
        retired.identity.last_sibling_drop.unwrap().child_identity,
        factory.payer
    );
    assert!(!original.sample().alive);
    assert_eq!(parent.retained_bytes_for_test(), first.parent_before);
    assert_eq!(db.accountant().usage().used, writer_before);
}

// Policy 21 late-admission row: early zero-headroom refusal never reaches the
// already-produced payload. The named decisive mutant refunds PendingTrace's
// payload before destroying its buffers when the genuine Box callback refuses.
#[test]
#[serial_test::serial]
fn retained_trace_late_box_refusal_frees_payload_before_returning_original_invoice() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let (_directory, db) = fixture();
    let parent = db.declared_read_memory_for_test();
    let allowance = parent.effective_limit_for_physical_test();
    let writer_before = db.accountant().usage().used;
    let _capture = Capture::begin();
    let (answer, first) = statement::observe(&parent, allowance, || query(&db));
    assert_eq!(answer.unwrap().rows(), vec![vec![Value::Int64(0)]]);
    let old = invoice(&db);
    let controls = assert_owner_request(OWNER.with(Cell::get), old, &first);
    assert_invoice(old, &first, controls);
    PRESSURE_PARENT.with(|slot| *slot.borrow_mut() = Some(parent.clone()));
    REFUSE_BOX.with(|slot| slot.set(true));
    let (answer, failed) = statement::observe(&parent, allowance, || query(&db));
    // capture969 begin
    dump_ordered_cuts("late-box-post-query-before-assertions");
    eprintln!(
        "capture969 observed-controls={:?} produced={:?}",
        failed.trace_owner_controls, failed.trace_produced
    );
    // capture969 end
    assert!(
        !REFUSE_BOX.with(Cell::get),
        "the real handoff must reach its Box callback"
    );
    let (_, token) = crate::executor::bounded::retained_trace_payload_layouts_for_test();
    assert!(
        matches!(answer, Err(Error::MemoryBudgetExceeded {
        requested_bytes, available_bytes: 0, budget_limit_bytes, ..
    }) if requested_bytes == token.size() && budget_limit_bytes == usize::try_from(allowance).unwrap()),
        "the actual prepaid Box admission, with zero remaining parent bytes, must refuse: {answer:?}"
    );
    let produced = failed
        .trace_produced
        .expect("the real producer reached late handoff");
    let factory = OWNER.with(Cell::get);
    let controls = assert_owner_request(factory, produced, &failed);
    assert_eq!(failed.trace_cleared, Some(old));
    assert_eq!(failed.trace_new, None);
    assert!(
        db.last_vector_search_trace.read().is_none(),
        "entered search already cleared its old slot; late refusal publishes no replacement"
    );
    let pending = ROLLBACK_TRACE.with(Cell::get).unwrap();
    assert_eq!(pending.allocations, produced.allocations);
    assert_eq!(pending.payload_layout_bytes(), pending.prepaid_bytes);
    let handoff = HANDOFF.with(Cell::get);
    assert_eq!(handoff.counter, factory.counter);
    assert_eq!(handoff.payer, factory.payer);
    assert_eq!(
        handoff.before, factory.after,
        "transferring the real payload credit does not change its genuine child debit"
    );
    assert_eq!(
        handoff.after,
        handoff.before - pending.prepaid_bytes,
        "late callback refusal returns only the real pending payload debit at this cut"
    );
    let original = vector_read_memory::original_child_observer_for_test(factory.payer).unwrap();
    assert!(original.sample().alive);
    let payload_count = pending
        .allocations
        .iter()
        .filter(|allocation| allocation.1 != 0)
        .count();
    let count = payload_count + 2;
    let freed = oracle::source_free_for_test(count);
    for (index, free) in freed[..count].iter().enumerate() {
        assert_eq!(
            free.2, 1,
            "each failed payload/provider/keeper must physically free once"
        );
        if index < payload_count {
            assert_eq!(
                (free.0, free.1),
                (handoff.before, handoff.before),
                "pending payload AND original owner controls survive both sides of native free"
            );
        } else {
            // This provider dies while the actual request is still live.
            // Its standing debit has not yet returned; unlike the retained-slot
            // lifecycle, it must be included at BOTH owner-control frees.
            let exact = if index == payload_count {
                handoff.after
            } else {
                handoff.after - controls.provider.1
            };
            assert_eq!(
                (free.0, free.1),
                (exact, exact),
                "late callback refusal preserves the full standing request plus provider/keeper debit through their native frees"
            );
        }
    }
    assert_eq!(counter_value(factory.counter), 0);
    assert_eq!(original.sample().charged_bytes, 0);
    assert_eq!(
        failed.identity.target_reserves, failed.identity.target_releases,
        "unrelated parent pressure cannot conceal a target-child leak or an early credit"
    );
    assert!(!failed.identity.target_drop_seen);
    // Return the pressure debit separately; it never paid the target child.
    // capture969 begin
    ordered_pressure("pressure-release-next");
    // capture969 end
    // capture969 begin
    dump_ordered_cuts("before-pressure-release");
    // capture969 end
    PRESSURE_CREDIT.with(|slot| drop(slot.borrow_mut().take()));
    assert_eq!(
        parent.retained_bytes_for_test(),
        first.parent_before + failed.control_bytes as u64
    );
    // capture969 begin
    ordered_cut(
        "pressure-released",
        [
            parent.retained_bytes_for_test() as usize,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
        ],
    );
    dump_ordered_cuts("after-pressure-release");
    // capture969 end
    oracle::disarm_source_free_watch();
    let (_, retired) = statement::observe(&parent, allowance, || {
        // capture969 begin
        ordered_stop();
        // capture969 end
        ROLLBACK_PIN.with(|slot| drop(slot.borrow_mut().take()));
    });
    assert_eq!(retired.drops, 1);
    assert_eq!(
        retired.identity.last_sibling_drop.unwrap().child_identity,
        factory.payer
    );
    assert!(!original.sample().alive);
    assert_eq!(parent.retained_bytes_for_test(), first.parent_before);
    assert_eq!(db.accountant().usage().used, writer_before);
}

// Policy 21 exception-flow row: a returned refusal and caller-after-return
// unwind cannot exercise PendingTrace while its genuine provider is unwinding.
// All state here belongs to this one-shot test, independently of old rows.
const PROVIDER_CALLBACK_PANIC: &str = "retained trace prepaid control provider unwind";
#[derive(Clone, Copy, Debug, Default)]
struct CallbackEntry {
    phase: &'static str,
    reason: &'static str,
    bytes: usize,
    counter: usize,
    payer: usize,
    held: usize,
}
thread_local! {
    static UNWIND_PROVIDER: Cell<bool> = const { Cell::new(false) };
    static CALLBACK_ENTRY: Cell<Option<CallbackEntry>> = const { Cell::new(None) };
}
struct ProviderUnwind;
impl ProviderUnwind {
    fn arm() -> Self {
        CALLBACK_ENTRY.with(|slot| slot.set(None));
        assert!(!UNWIND_PROVIDER.with(|slot| slot.replace(true)));
        Self
    }
}
impl Drop for ProviderUnwind {
    fn drop(&mut self) {
        UNWIND_PROVIDER.with(|slot| slot.set(false));
        CALLBACK_ENTRY.with(|slot| slot.set(None));
    }
}
// Called only by the cfg(test + test-seams) hook in the real provider, before
// reservation. Exact borrowed discriminants; no formatting or subject data.
pub(crate) fn provider_callback_unwind(
    reader: &ReadMemoryOwner,
    bytes: usize,
    phase: &str,
    reason: &str,
) {
    if phase != "vector_copy"
        || reason != "prepaid_control"
        || !UNWIND_PROVIDER.with(|slot| slot.replace(false))
    {
        return;
    }
    let pending = PENDING
        .with(Cell::get)
        .expect("actual producer reached provider callback");
    ROLLBACK_TRACE.with(|slot| slot.set(Some(pending)));
    ROLLBACK_PIN.with(|slot| {
        assert!(
            slot.borrow_mut()
                .replace(OriginalPin::new(reader))
                .is_none()
        )
    });
    let counter = vector_read_memory::declared_child_observer(reader)
        .unwrap()
        .physical_funding_counter_for_test();
    let factory = OWNER.with(Cell::get);
    assert_eq!(factory.count, 2);
    let mut requests = [(0, 0, 0); 9];
    let mut count = 0;
    for allocation in pending.allocations {
        if allocation.1 != 0 {
            requests[count] = allocation;
            count += 1;
        }
    }
    assert!(
        count > 0,
        "callback unwind must destroy actual producer buffers"
    );
    requests[count] = factory.allocations[1];
    count += 1;
    requests[count] = factory.allocations[0];
    count += 1;
    watch_rollback_native(&mut requests, count, reader, counter);
    CALLBACK_ENTRY.with(|slot| {
        slot.set(Some(CallbackEntry {
            phase: "vector_copy",
            reason: "prepaid_control",
            bytes,
            counter,
            payer: vector_read_memory::observed_reader_identity(reader),
            held: counter_value(counter),
        }))
    });
    // No TLS borrow or telemetry callback remains on the stack at this panic.
    panic!("retained trace prepaid control provider unwind");
}

#[test]
#[serial_test::serial]
fn retained_trace_provider_callback_unwind_keeps_pending_payload_paid_through_native_frees() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let (_directory, db) = fixture();
    let parent = db.declared_read_memory_for_test();
    let allowance = parent.effective_limit_for_physical_test();
    let writer_before = db.accountant().usage().used;
    let _capture = Capture::begin();
    let (answer, first) = statement::observe(&parent, allowance, || query(&db));
    assert_eq!(answer.unwrap().rows(), vec![vec![Value::Int64(0)]]);
    let old = invoice(&db);
    let controls = assert_owner_request(OWNER.with(Cell::get), old, &first);
    assert_invoice(old, &first, controls);
    let _fault = ProviderUnwind::arm();
    let (caught, failed) = statement::observe(&parent, allowance, || {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| query(&db)))
    });
    // capture969 begin
    dump_ordered_cuts("provider-unwind-post-query-before-assertions");
    eprintln!(
        "capture969 observed-controls={:?} produced={:?}",
        failed.trace_owner_controls, failed.trace_produced
    );
    // capture969 end
    let panic = caught.expect_err("the actual provider callback must unwind");
    assert_eq!(
        panic.downcast_ref::<&str>(),
        Some(&PROVIDER_CALLBACK_PANIC),
        "only the fixed provider fault qualifies; a telemetry borrow panic does not"
    );
    assert!(
        !UNWIND_PROVIDER.with(Cell::get),
        "the real callback consumes its fault once"
    );
    let entered = CALLBACK_ENTRY
        .with(Cell::get)
        .expect("actual callback entry recorded");
    assert_eq!(
        (entered.phase, entered.reason),
        ("vector_copy", "prepaid_control")
    );
    let (_, token) = crate::executor::bounded::retained_trace_payload_layouts_for_test();
    assert_eq!(
        entered.bytes,
        token.size(),
        "fault precedes the genuine token Box reservation"
    );
    let produced = failed
        .trace_produced
        .expect("genuine producer completed before callback unwind");
    let factory = OWNER.with(Cell::get);
    let controls = assert_owner_request(factory, produced, &failed);
    assert_eq!(failed.trace_cleared, Some(old));
    assert_eq!(failed.trace_new, None);
    assert!(db.last_vector_search_trace.read().is_none());
    let pending = ROLLBACK_TRACE.with(Cell::get).unwrap();
    assert_eq!(
        pending.allocations, produced.allocations,
        "observe original producer pointers and Layouts, without a payload copy"
    );
    assert_eq!(pending.payload_layout_bytes(), pending.prepaid_bytes);
    assert!(pending.prepaid_bytes > 0);
    let handoff = HANDOFF.with(Cell::get);
    assert_eq!(entered.payer, factory.payer);
    assert_eq!(entered.counter, factory.counter);
    assert_eq!(entered.held, factory.after);
    assert_eq!(handoff.payer, factory.payer);
    assert_eq!(handoff.counter, factory.counter);
    assert_eq!(
        handoff.before, entered.held,
        "moving payload credit into PendingTrace does not refund the original child"
    );
    assert_eq!(
        handoff.after,
        handoff.before - pending.prepaid_bytes,
        "callback unwind refunds payload only after PendingTrace destroys its buffers"
    );
    let original = vector_read_memory::original_child_observer_for_test(factory.payer).unwrap();
    assert!(
        original.sample().alive,
        "the explicit original-child pin protects every counter read"
    );
    let payload_count = pending
        .allocations
        .iter()
        .filter(|allocation| allocation.1 != 0)
        .count();
    let count = payload_count + 2;
    let freed = oracle::source_free_for_test(count);
    for (index, free) in freed[..count].iter().enumerate() {
        assert_eq!(
            free.2, 1,
            "each original payload/provider/keeper physically frees exactly once"
        );
        let exact = if index < payload_count {
            entered.held
        } else if index == payload_count {
            handoff.after
        } else {
            handoff.after - controls.provider.1
        };
        assert_eq!(
            (free.0, free.1),
            (exact, exact),
            "full original payload and standing request stay paid through buffer frees; provider and keeper stay paid at their later cuts"
        );
    }
    assert_eq!(counter_value(factory.counter), 0);
    assert_eq!(original.sample().charged_bytes, 0);
    assert_eq!(
        failed.identity.target_reserves,
        failed.identity.target_releases
    );
    assert_eq!(failed.identity.target_parent_net, 0);
    assert!(
        !failed.identity.target_drop_seen,
        "native frees complete before deliberate pin retirement"
    );
    assert_eq!(
        failed.parent_after,
        first.parent_before + failed.control_bytes as u64
    );
    oracle::disarm_source_free_watch();
    let (_, retired) = statement::observe(&parent, allowance, || {
        // capture969 begin
        ordered_stop();
        // capture969 end
        ROLLBACK_PIN.with(|slot| drop(slot.borrow_mut().take()));
    });
    assert_eq!(retired.drops, 1);
    assert_eq!(
        retired.identity.last_sibling_drop.unwrap().child_identity,
        factory.payer
    );
    assert!(!original.sample().alive);
    assert_eq!(parent.retained_bytes_for_test(), first.parent_before);
    assert_eq!(db.accountant().usage().used, writer_before);
    // std panic and test-harness runtime allocations are outside this narrow
    // original-native-free proof; no process allocator/OOM qualification follows.
}
