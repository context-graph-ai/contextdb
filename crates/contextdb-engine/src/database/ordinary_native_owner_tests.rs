//! Ordinary SQL must retain the original native child and both forwarding
//! allocations through physical free. These observations borrow original
//! counters; they do not create an owner, reserve bytes, or alter a limit.
use super::*;
use crate::persistence::content_identity::proof_allocation_oracle_tests as oracle;
use crate::vector_read_memory::{DeclaredReadMemory, ordinary_native_child_layout_for_test};
use contextdb_core::read_memory::{NativeRefusal, ReadCredit, ReadShared};
use std::alloc::Layout;
use std::cell::Cell;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU64, AtomicUsize};

type Request = (usize, usize, usize);
#[derive(Clone, Copy, Default)]
struct Capture {
    active: bool,
    invalid: bool,
    invalid_site: u32,
    actual_constructor_requests: [(usize, usize, usize, usize); 3],
    parent: usize,
    parent_counter: usize,
    baseline: u64,
    children: usize,
    stages: usize,
    allow_constructor_prefix: bool,
    constructor_base: usize,
    requests: [Request; 3], // native child, external keeper, forwarding Arc
    trace_requests: [Request; 2], // independently observed keeper, provider
    trace_ordinals: [usize; 2],
    trace_invoices: [(usize, usize); 2],
    trace_stages: usize,
    trace_before: usize,
    trace_parent: [usize; 2],
    trace_credits: [usize; 2],
    child_limit: u64,
    declared_limit: u64,
    parent_after_constructor: [u64; 3],
    allocator_funding: [usize; 3],
    child_payload: usize,
    child_counter: usize,
    child_at_keeper: u64,
    child_at_forwarding: u64,
    payer: usize,
    keeper_payload: usize,
    outer_invoice_payload: usize,
    inner_invoice_payload: usize,
    forwarding_payload: usize,
    payload_invoice_bytes: usize,
}
#[derive(Clone, Copy, Default)]
struct AnswerTraceCapture {
    active: bool,
    invalid: bool,
    stages: usize,
    requests: [Request; 2],
    invoices: [(usize, usize); 2],
}
thread_local! { static ANSWER_TRACE: Cell<AnswerTraceCapture> = const { Cell::new(AnswerTraceCapture {
    active:false, invalid:false, stages:0, requests:[(0,0,0);2], invoices:[(0,0);2],
}) }; }
thread_local! {
    static HOOK_BUSY: Cell<bool> = const { Cell::new(false) };
    static CAPTURE: Cell<Capture> = const { Cell::new(Capture {
        active: false, invalid: false, invalid_site: 0, actual_constructor_requests: [(0,0,0,0);3], parent: 0, parent_counter: 0,
        baseline: 0, children: 0, stages: 0, allow_constructor_prefix: false, constructor_base: 0, requests: [(0, 0, 0); 3],
        trace_requests: [(0,0,0); 2], trace_ordinals: [0; 2], trace_invoices: [(0,0); 2], trace_stages: 0,
        trace_before: 0, trace_parent: [0; 2], trace_credits: [0; 2],
        child_limit: 0, declared_limit: 0, parent_after_constructor: [0; 3], allocator_funding: [0; 3],
        child_payload: 0, child_counter: 0, child_at_keeper: 0,
        child_at_forwarding: 0, payer: 0, keeper_payload: 0,
        outer_invoice_payload: 0, inner_invoice_payload: 0,
        forwarding_payload: 0, payload_invoice_bytes: 0,
    }) };
}

// Reentry or a fourth selected constructor is a failed precondition, never
// silently truncated evidence. This guard and the receipt are fixed scalars.
fn update(action: impl FnOnce(&mut Capture)) {
    CAPTURE.with(|slot| {
        let mut c = slot.get();
        if !c.active {
            return;
        }
        if HOOK_BUSY.with(|busy| busy.replace(true)) {
            c.invalid = true;
            if c.invalid_site == 0 {
                c.invalid_site = line!();
            }
            slot.set(c);
            return;
        }
        struct Exit;
        impl Drop for Exit {
            fn drop(&mut self) {
                HOOK_BUSY.with(|busy| busy.set(false));
            }
        }
        let _exit = Exit;
        action(&mut c);
        c.invalid |= slot.get().invalid;
        slot.set(c);
    });
}

// The actual global allocator supplies the request, including its alignment.
// A mismatched pointer/layout/stage invalidates this observation before any
// oracle header is inspected. No matching-by-Layout attribution is accepted.
fn record(c: &mut Capture, stage: usize, payload: usize, expected: Layout) {
    let inventory = crate::executor::bounded::ordinary_native_control_layouts_for_test();
    if inventory.get(stage).copied() != Some(expected) {
        c.invalid = true;
        if c.invalid_site == 0 {
            c.invalid_site = line!();
        }
        return;
    }
    let (bytes, align, ordinal) = oracle::last_request_for_test(false);
    let pointer = oracle::last_request_pointer_for_test();
    c.actual_constructor_requests[stage] = (pointer, bytes, align, ordinal);
    if c.allow_constructor_prefix && stage == 0 {
        let Some(base) = ordinal.checked_sub(1).filter(|base| *base <= 256 - 3) else {
            c.invalid = true;
            c.invalid_site = line!();
            return;
        };
        c.constructor_base = base;
    }
    if c.stages != stage
        || pointer == 0
        || ordinal != c.constructor_base + stage + 1
        || (bytes, align) != (expected.size(), expected.align())
        || payload < pointer
        || payload >= pointer.saturating_add(bytes)
    {
        c.invalid = true;
        if c.invalid_site == 0 {
            c.invalid_site = line!();
        }
        return;
    }
    c.requests[stage] = (pointer, bytes, align);
    // SAFETY: this scope is nested inside execute on a live Database; it
    // never outlives that Database's original declared parent.
    c.parent_after_constructor[stage] =
        unsafe { &*(c.parent_counter as *const AtomicU64) }.load(Ordering::SeqCst);
    c.stages += 1;
    // Existing physical-free oracle, stable ORIGINAL parent counter. This
    // target has checked equal atomic width/alignment before entering here.
    // Each extension happens before any of the preceding allocations can die.
    oracle::watch_exact_frees_for_test(&c.requests[..c.stages], c.parent_counter);
}

pub(crate) fn trace_admission_begin(payer: usize) {
    update(|c| {
        if payer != c.payer || c.trace_stages != 0 {
            c.invalid = true;
            if c.invalid_site == 0 {
                c.invalid_site = line!();
            }
            return;
        }
        c.trace_before =
            unsafe { &*(c.parent_counter as *const AtomicU64) }.load(Ordering::SeqCst) as usize;
    });
}
// Fixed PREALLOCATIONADMIT scalar: sampled after the genuine invoice
// admits and before its physical request. This is not the original allocator's
// first-256 pre-System sample, which does not cover these late trace requests.
pub(crate) fn trace_control_preallocation(stage: usize, payer: usize, invoice_bytes: usize) {
    update(|c| {
        if stage >= 2 || payer != c.payer || invoice_bytes == 0 {
            c.invalid = true;
            if c.invalid_site == 0 {
                c.invalid_site = line!();
            }
            return;
        }
        c.trace_credits[stage] =
            unsafe { &*(c.parent_counter as *const AtomicU64) }.load(Ordering::SeqCst) as usize;
    });
}

pub(crate) fn trace_control_allocated(
    stage: usize,
    payload: usize,
    layout: Layout,
    payer: usize,
    invoice: usize,
) {
    ANSWER_TRACE.with(|slot| {
        let mut capture = slot.get();
        if !capture.active {
            return;
        }
        let (bytes, align, _) = oracle::last_request_for_test(false);
        let pointer = oracle::last_request_pointer_for_test();
        if stage >= 2
            || stage != capture.stages
            || pointer == 0
            || payer == 0
            || (bytes, align) != (layout.size(), layout.align())
            || bytes != invoice
            || payload < pointer
            || payload >= pointer.saturating_add(bytes)
        {
            capture.invalid = true;
        } else {
            capture.requests[stage] = (pointer, bytes, align);
            capture.invoices[stage] = (payer, invoice);
            capture.stages += 1;
        }
        slot.set(capture);
    });
    update(|c| {
        let (bytes, align, ordinal) = oracle::last_request_for_test(false);
        let pointer = oracle::last_request_pointer_for_test();
        if c.trace_stages != stage
            || stage >= 2
            || c.stages != 3
            || payer != c.payer
            || pointer == 0
            || invoice != layout.size()
            || (bytes, align) != (layout.size(), layout.align())
            || payload < pointer
            || payload >= pointer.saturating_add(bytes)
            || ordinal <= c.constructor_base + 3
            || (stage == 1 && ordinal != c.trace_ordinals[0] + 1)
        {
            c.invalid = true;
            if c.invalid_site == 0 {
                c.invalid_site = line!();
            }
            return;
        }
        if oracle::source_free_for_test(c.stages + c.trace_stages)[..c.stages + c.trace_stages]
            .iter()
            .any(|free| free.2 != 0)
        {
            c.invalid = true;
            if c.invalid_site == 0 {
                c.invalid_site = line!();
            }
            return;
        }
        c.trace_requests[stage] = (pointer, bytes, align);
        c.trace_invoices[stage] = (payer, invoice);
        c.trace_ordinals[stage] = ordinal;
        c.trace_parent[stage] =
            unsafe { &*(c.parent_counter as *const AtomicU64) }.load(Ordering::SeqCst) as usize;
        c.trace_stages += 1;
        let mut requests = [(0, 0, 0); 5];
        requests[..3].copy_from_slice(&c.requests);
        requests[3..3 + c.trace_stages].copy_from_slice(&c.trace_requests[..c.trace_stages]);
        oracle::watch_exact_frees_for_test(&requests[..3 + c.trace_stages], c.parent_counter);
    });
}

pub(crate) fn child_allocated(
    parent: &DeclaredReadMemory,
    counter: &AtomicU64,
    pointer: usize,
    layout: Layout,
) {
    update(|c| {
        c.children += 1;
        if c.parent != parent as *const _ as usize {
            c.invalid = true;
            if c.invalid_site == 0 {
                c.invalid_site = line!();
            }
        }
        if c.parent_counter != counter as *const _ as usize {
            c.invalid = true;
            if c.invalid_site == 0 {
                c.invalid_site = line!();
            }
        }
        record(c, 0, pointer, layout);
        if c.requests[0].0 != pointer {
            c.invalid = true;
            if c.invalid_site == 0 {
                c.invalid_site = line!();
            }
        }
    });
}
pub(crate) fn child_counter(
    parent: &DeclaredReadMemory,
    payload: usize,
    counter: &AtomicU64,
    limit: u64,
) {
    update(|c| {
        if c.parent != parent as *const _ as usize
            || (c.child_counter != 0 && c.child_counter != counter as *const _ as usize)
        {
            c.invalid = true;
            if c.invalid_site == 0 {
                c.invalid_site = line!();
            }
        }
        if c.child_limit != 0 && c.child_limit != limit {
            c.invalid = true;
            if c.invalid_site == 0 {
                c.invalid_site = line!();
            }
        }
        c.child_limit = limit;
        c.child_payload = payload;
        c.child_counter = counter as *const _ as usize;
    });
}
pub(crate) fn keeper_allocated(
    owner: usize,
    payload: usize,
    payer: usize,
    payload_bytes: usize,
    layout: Layout,
) {
    update(|c| {
        c.payer = payer;
        c.keeper_payload = payload;
        c.payload_invoice_bytes = payload_bytes;
        if owner != c.requests[0].0 || payer != owner || c.child_counter == 0 {
            c.invalid = true;
            if c.invalid_site == 0 {
                c.invalid_site = line!();
            }
        } else {
            // SAFETY: construction currently borrows the live native child.
            c.child_at_keeper =
                unsafe { &*(c.child_counter as *const AtomicU64) }.load(Ordering::SeqCst);
        }
        record(c, 1, payload, layout);
    });
}
pub(crate) fn forwarding_allocated(
    owner: usize,
    payload: usize,
    outer_invoice: usize,
    inner_invoice: usize,
    layout: Layout,
) {
    update(|c| {
        c.forwarding_payload = payload;
        c.outer_invoice_payload = outer_invoice;
        c.inner_invoice_payload = inner_invoice;
        if owner != c.payer || c.child_counter == 0 {
            c.invalid = true;
            if c.invalid_site == 0 {
                c.invalid_site = line!();
            }
        } else {
            // SAFETY: this constructor still owns the native child.
            c.child_at_forwarding =
                unsafe { &*(c.child_counter as *const AtomicU64) }.load(Ordering::SeqCst);
        }
        record(c, 2, payload, layout);
    });
}

struct Observation;
// This validates copies of independent allocator/invoice receipts. It never
// reads a current usage balance or alters an engine/allocator observation.
#[derive(Clone, Copy)]
struct OriginalControlCut {
    request: Request,
    original_payer: usize,
    invoice_payer: usize,
    invoice_bytes: usize,
    free: (usize, usize, usize),
    independent_live: usize,
    baseline: usize,
}
fn original_control_cut_is_complete(cut: OriginalControlCut) -> bool {
    cut.request.0 != 0
        && cut.request.1 != 0
        && cut.request.2.is_power_of_two()
        && cut.original_payer != 0
        && cut.invoice_payer == cut.original_payer
        && cut.invoice_bytes == cut.request.1
        && cut.free.2 == 1
        && cut.free.0 == cut.free.1
        && cut.independent_live >= cut.request.1
        && cut.baseline.checked_add(cut.independent_live) == Some(cut.free.0)
}

impl Observation {
    fn begin(db: &Database) -> Self {
        // The existing exact-free oracle reads AtomicUsize. Do not qualify a
        // target where that read would differ from this real AtomicU64 counter.
        assert_eq!(
            std::mem::size_of::<AtomicU64>(),
            std::mem::size_of::<AtomicUsize>()
        );
        assert_eq!(
            std::mem::align_of::<AtomicU64>(),
            std::mem::align_of::<AtomicUsize>()
        );
        CAPTURE.with(|slot| {
            assert!(
                !slot.get().active,
                "nested observation invalidates qualification"
            );
            slot.set(Capture {
                active: true,
                parent: db.declared_read_memory.as_ref() as *const DeclaredReadMemory as usize,
                parent_counter: db.declared_read_memory.ordinary_parent_counter_for_test(),
                baseline: db.declared_read_memory.charged_bytes(),
                declared_limit: db.declared_read_memory.effective_limit_for_physical_test(),
                ..Capture::default()
            });
        });
        oracle::last_request_for_test(true);
        oracle::disarm_source_free_watch();
        Self
    }
    fn begin_at_actual_native_constructor(db: &Database) -> Self {
        let observer = Self::begin(db);
        update(|c| c.allow_constructor_prefix = true);
        observer
    }
    fn capture(&self) -> Capture {
        let c = CAPTURE.with(Cell::get);
        assert!(
            !c.invalid,
            "constructor receipt invalid at line {}; children={} stages={} trace_stages={} actual={:?} expected={:?}",
            c.invalid_site,
            c.children,
            c.stages,
            c.trace_stages,
            c.actual_constructor_requests,
            c.requests
        );
        c
    }
    fn assert_admitted(&self) -> Capture {
        let c = self.capture();
        assert_eq!((c.children, c.stages), (1, 3));
        assert_eq!(
            c.child_limit, c.declared_limit,
            "ordinary child must preserve the genuine parent limit"
        );
        assert_eq!(
            c.payer, c.requests[0].0,
            "both control credits use the native block"
        );
        assert!(
            c.child_payload >= c.requests[0].0
                && c.child_payload < c.requests[0].0 + c.requests[0].1
        );
        assert_eq!(c.outer_invoice_payload, c.keeper_payload);
        assert_eq!(
            c.inner_invoice_payload, c.keeper_payload,
            "forwarder and outer scope retain the SAME original invoice"
        );
        assert_eq!(c.payload_invoice_bytes, c.requests[2].1);
        let controls = (c.requests[1].1 + c.requests[2].1) as u64;
        assert_eq!(
            c.child_at_keeper, controls,
            "admit both controls before allocating the keeper"
        );
        assert_eq!(c.child_at_forwarding, controls);
        for stage in 0..3 {
            assert_eq!(
                c.allocator_funding[stage] as u64, c.parent_after_constructor[stage],
                "the original parent must hold the same debit BEFORE System.alloc"
            );
        }
        assert_eq!(
            c.parent_after_constructor[0],
            c.baseline + c.requests[0].1 as u64
        );
        for stage in [1, 2] {
            assert_eq!(
                c.parent_after_constructor[stage],
                c.baseline + c.requests[0].1 as u64 + controls
            );
        }
        c
    }
    fn assert_freed(&self, db: &Database) {
        let c = self.capture();
        let count = c.stages + c.trace_stages;
        assert!(
            c.trace_stages == 0 || c.trace_stages == 2,
            "both actual trace controls must qualify"
        );
        if c.trace_stages == 2 {
            let trace_bytes = c
                .trace_requests
                .iter()
                .map(|request| request.1)
                .sum::<usize>();
            for stage in 0..2 {
                assert_eq!(c.trace_parent[stage], c.trace_before + trace_bytes);
                assert_eq!(
                    c.trace_credits[stage], c.trace_parent[stage],
                    "PREALLOCATIONADMIT and actual-allocation samples retain the same original parent debit"
                );
            }
        }
        let frees = oracle::source_free_for_test(count);
        let live = oracle::source_live_at_free_for_test(count);
        for (i, free) in frees[..count].iter().enumerate() {
            assert_eq!(
                free.2, 1,
                "selected allocation {i} must physically free exactly once"
            );
            assert_eq!(
                free.0, free.1,
                "original counter must not refund inside physical free"
            );
            let request = if i < c.stages {
                c.requests[i]
            } else {
                c.trace_requests[i - c.stages]
            };
            assert_eq!(
                c.baseline as usize + live[i],
                free.0,
                "every independently still-live original control is paid at its physical free"
            );
            if i >= c.stages {
                let (invoice_payer, invoice_bytes) = c.trace_invoices[i - c.stages];
                assert!(
                    original_control_cut_is_complete(OriginalControlCut {
                        request,
                        original_payer: c.payer,
                        invoice_payer,
                        invoice_bytes,
                        free: *free,
                        independent_live: live[i],
                        baseline: c.baseline as usize,
                    }),
                    "actual allocation-time trace invoice must match its independent physical request"
                );
            }
        }
        assert_eq!(db.declared_read_memory.charged_bytes(), c.baseline);
    }
    fn assert_live(&self) {
        let c = self.capture();
        let frees = oracle::source_free_for_test(c.stages + c.trace_stages);
        for free in &frees[..c.stages + c.trace_stages] {
            assert_eq!(free.2, 0);
        }
    }
}
impl Drop for Observation {
    fn drop(&mut self) {
        CAPTURE.with(|slot| slot.set(Capture::default()));
        oracle::disarm_source_free_watch();
        // Restore a selected null fault even if a fixture precondition panics.
        let _ = crate::vector_read_memory::native_ownership_proof::refuse_selected_live_layout(
            ordinary_native_child_layout_for_test(),
        );
        let _ = crate::vector_read_memory::native_ownership_proof::refuse_selected_live_layout(
            ReadShared::<ReadCredit>::layout(),
        );
    }
}

// Reuse the existing pre-System.alloc funding sampler on the ORIGINAL parent
// counter, not a mirrored balance. Only the three selected constructor requests
// are consumed; later ordinary query allocations are outside this receipt.
fn observed<T>(db: &Database, action: impl FnOnce() -> T) -> (T, [Request; 3], usize) {
    let (value, requests, count) = oracle::source_requests_for_test(
        db.declared_read_memory.ordinary_parent_counter_for_test(),
        action,
    );
    let credits = oracle::source_request_credit_for_test();
    update(|c| {
        if c.stages > 3 || count < c.constructor_base + c.stages {
            c.invalid = true;
            if c.invalid_site == 0 {
                c.invalid_site = line!();
            }
            return;
        }
        for stage in 0..c.stages {
            let at = c.constructor_base + stage;
            if at >= requests.len() || c.requests[stage] != requests[at] {
                c.invalid = true;
                if c.invalid_site == 0 {
                    c.invalid_site = line!();
                }
            }
            c.allocator_funding[stage] = credits[at];
        }
        for stage in 0..c.trace_stages {
            let ordinal = c.trace_ordinals[stage];
            if ordinal == 0 || ordinal > count || c.trace_credits[stage] == 0 {
                c.invalid = true;
                if c.invalid_site == 0 {
                    c.invalid_site = line!();
                }
                return;
            }
            // Pointer/Layout are independently captured from the allocator's
            // actual last request in trace_control_allocated, not reconstructed
            // through the original first-256 funding recorder.
        }
    });
    let captured = CAPTURE.with(Cell::get);
    if captured.allow_constructor_prefix && captured.constructor_base != 0 {
        eprintln!(
            "native_constructor_prefix requests={:?} original_parent_funding={:?}",
            &requests[..captured.constructor_base],
            &credits[..captured.constructor_base]
        );
    }
    (value, captured.requests, count)
}

fn fixture(file: bool) -> (tempfile::TempDir, Database) {
    let dir = tempfile::tempdir().unwrap();
    let db = if file {
        Database::open_with_options(
            dir.path().join("ordinary.db"),
            DatabaseOpenOptions {
                maintenance_policy: MaintenancePolicy::CallerDriven,
                ..DatabaseOpenOptions::default()
            },
        )
        .unwrap()
    } else {
        Database::open_memory()
    };
    (dir, db)
}
fn sql(db: &Database, statement: &str) -> QueryResult {
    db.execute(statement, &HashMap::new()).unwrap()
}
fn entry(
    db: &Database,
    historical: bool,
    statement: &str,
    params: &HashMap<String, Value>,
    snapshot: SnapshotId,
) -> Result<QueryResult> {
    if historical {
        db.execute_at_snapshot(statement, params, snapshot)
    } else {
        db.execute(statement, params)
    }
}

#[derive(Default)]
struct Routes {
    public: AtomicUsize,
    plans: AtomicUsize,
    pull: AtomicUsize,
    drained: AtomicUsize,
    eager: AtomicUsize,
}
impl ReadExecutionConvergenceObserver for Routes {
    fn observe(&self, event: ReadExecutionConvergenceEvent) {
        let counter = match event {
            ReadExecutionConvergenceEvent::PublicExecuteEntered => &self.public,
            ReadExecutionConvergenceEvent::PlanReady => &self.plans,
            ReadExecutionConvergenceEvent::PullKernelEntered => &self.pull,
            ReadExecutionConvergenceEvent::PullKernelDrained => &self.drained,
            ReadExecutionConvergenceEvent::EagerRowSourceTouch => &self.eager,
            _ => return,
        };
        counter.fetch_add(1, Ordering::SeqCst);
    }
}

// Acceptance: actual ordinary SQL + two distinct historical answers, with
// original native allocation/payer evidence on BOTH backing implementations.
// Decisive mutants: legacy child acquisition, parent-as-operation-owner,
// snapshot ignored, or adapter/keeper refunded before native deallocation.
#[test]
#[serial_test::serial]
fn ordinary_sql_and_two_snapshots_retain_the_original_native_parent() {
    let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_800_000_000_000);
    for file in [false, true] {
        let (_dir, db) = fixture(file);
        sql(
            &db,
            "CREATE TABLE readings (id INT PRIMARY KEY, revision INT)",
        );
        sql(&db, "INSERT INTO readings VALUES (1, 10)");
        let old = db.snapshot();
        sql(&db, "UPDATE readings SET revision = 20 WHERE id = 1");
        let new = db.snapshot();
        assert_ne!(old, new, "fixture needs two committed snapshots");
        let query = "SELECT revision FROM readings WHERE id = 1";
        for (historical, snapshot, expected) in [(false, new, 20), (true, old, 10), (true, new, 20)]
        {
            let routes = Arc::new(Routes::default());
            let observation = Observation::begin(&db);
            let (answer, _, _) = observed(&db, || {
                db.with_read_execution_convergence_observer_for_test(routes.clone(), || {
                    entry(&db, historical, query, &HashMap::new(), snapshot)
                })
            });
            let answer = answer.unwrap();
            assert_eq!(answer.columns(), vec!["revision"]);
            assert_eq!(answer.rows(), vec![vec![Value::Int64(expected)]]);
            assert_eq!(answer.rows_affected(), 0);
            assert_eq!(routes.public.load(Ordering::SeqCst), 1);
            assert_eq!(routes.plans.load(Ordering::SeqCst), 1);
            assert_eq!(routes.pull.load(Ordering::SeqCst), 1);
            assert_eq!(routes.drained.load(Ordering::SeqCst), 1);
            assert_eq!(routes.eager.load(Ordering::SeqCst), 0);
            observation.assert_admitted();
            observation.assert_live();
            drop(answer);
            observation.assert_freed(&db);
        }
        // Cross the actual transport row default at its smallest boundary.
        // Ordinary SQL must still return every row with unchanged reader memory.
        let row_count =
            usize::try_from(contextdb_core::read_contract::ReadLimits::default().result_rows)
                .unwrap()
                .checked_add(1)
                .unwrap();
        sql(&db, "CREATE TABLE ordinary_rows (id INT PRIMARY KEY)");
        let values = (0..row_count)
            .map(|id| format!("({id})"))
            .collect::<Vec<_>>()
            .join(",");
        assert_eq!(
            sql(&db, &format!("INSERT INTO ordinary_rows VALUES {values}")).rows_affected(),
            row_count as u64
        );
        let snapshot = db.snapshot();
        let expected = (0..row_count)
            .map(|id| vec![Value::Int64(id as i64)])
            .collect::<Vec<_>>();
        for historical in [false, true] {
            let observation = Observation::begin(&db);
            let (answer, _, _) = observed(&db, || {
                entry(
                    &db,
                    historical,
                    "SELECT id FROM ordinary_rows ORDER BY id",
                    &HashMap::new(),
                    snapshot,
                )
            });
            assert_eq!(
                answer.unwrap().rows(),
                expected,
                "ordinary queries must not acquire transport's default result-row ceiling"
            );
            observation.assert_admitted();
            observation.assert_freed(&db);
        }
    }
}

// Acceptance: the native owner adaptation preserves the ordinary transaction
// overlay and the actual eager DML fallback; no alternate test executor is used.
// Decisive mutants: always force pull, lose tx overlay, change rollback/commit.
#[test]
#[serial_test::serial]
fn ordinary_transactions_preserve_eager_updates_and_private_rows() {
    let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_800_000_000_000);
    for file in [false, true] {
        let (_dir, db) = fixture(file);
        sql(
            &db,
            "CREATE TABLE readings (id INT PRIMARY KEY, revision INT)",
        );
        sql(&db, "INSERT INTO readings VALUES (1, 10)");
        sql(&db, "BEGIN");
        let routes = Arc::new(Routes::default());
        let observation = Observation::begin(&db);
        let (updated, _, _) = observed(&db, || {
            db.with_read_execution_convergence_observer_for_test(routes.clone(), || {
                db.execute(
                    "UPDATE readings SET revision = 30 WHERE revision = 10",
                    &HashMap::new(),
                )
            })
        });
        assert_eq!(updated.unwrap().rows_affected(), 1);
        assert_eq!(routes.public.load(Ordering::SeqCst), 1);
        assert_eq!(routes.plans.load(Ordering::SeqCst), 1);
        assert_eq!(routes.pull.load(Ordering::SeqCst), 0);
        assert_eq!(routes.drained.load(Ordering::SeqCst), 0);
        assert!(
            routes.eager.load(Ordering::SeqCst) > 0,
            "fixture must exercise the actual eager source"
        );
        observation.assert_admitted();
        observation.assert_freed(&db);
        drop(observation);
        assert_eq!(
            sql(&db, "SELECT revision FROM readings").rows(),
            vec![vec![Value::Int64(30)]]
        );
        sql(&db, "ROLLBACK");
        assert_eq!(
            sql(&db, "SELECT revision FROM readings").rows(),
            vec![vec![Value::Int64(10)]]
        );
        sql(&db, "BEGIN");
        sql(&db, "INSERT INTO readings VALUES (2, 40)");
        sql(&db, "COMMIT");
        assert_eq!(
            sql(&db, "SELECT revision FROM readings ORDER BY id").rows(),
            vec![vec![Value::Int64(10)], vec![Value::Int64(40)]]
        );
    }
}

fn borrowed_refusal(
    error: Error,
    subsystem_expected: &str,
    operation_expected: &str,
    requested: usize,
    available: usize,
    limit: u64,
) {
    let Error::MemoryBudgetExceeded {
        subsystem,
        operation,
        requested_bytes,
        available_bytes,
        budget_limit_bytes,
        hint,
    } = error
    else {
        panic!("ordinary admission must preserve the typed memory refusal");
    };
    assert_eq!(subsystem, subsystem_expected);
    assert_eq!(operation, operation_expected);
    let expected_hint = if subsystem_expected == "reader" {
        "Reduce simultaneous requests or raise the declared reader memory limit."
    } else {
        "Release retained read controls or raise MEMORY_LIMIT."
    };
    assert_eq!(hint, expected_hint);
    assert!(subsystem.static_text().is_some());
    assert!(operation.static_text().is_some());
    assert!(hint.static_text().is_some());
    assert_eq!(requested_bytes, requested);
    assert_eq!(available_bytes, available);
    assert_eq!(budget_limit_bytes, limit as usize);
}

// Acceptance: original-parent refusal precedes setup, keeper refusal refunds
// the acquired native block once, and existing native/keeper null seams return
// their declared error classes. Limits only LOWER the real parent ceiling.
// Decisive mutants: acquire after parse/setup, allocate before admission,
// owned diagnostic conversion, leaked/double-refunded child, new transport caps.
#[test]
#[serial_test::serial]
fn ordinary_admission_and_allocator_refusals_preserve_typed_fields_and_cleanup() {
    let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_800_000_000_000);
    for file in [false, true] {
        let (_dir, db) = fixture(file);
        let snapshot = db.snapshot();
        let child = ordinary_native_child_layout_for_test();
        let keeper = ReadShared::<ReadCredit>::layout();
        assert_ne!(
            child, keeper,
            "layout-selected null rows require distinguishable native child and keeper requests"
        );
        let baseline = db.declared_read_memory.charged_bytes();
        let genuine_limit = db.declared_read_memory.effective_limit_for_physical_test();
        assert!(
            baseline + child.size() as u64 + keeper.size() as u64 <= genuine_limit,
            "fixture must fit beneath its genuine unchanged parent ceiling"
        );
        for historical in [false, true] {
            // Malformed SQL would fail parsing if setup ran before admission.
            let limit = baseline + child.size() as u64 - 1;
            let observation = Observation::begin(&db);
            let ((answer, _), _, allocation_count) = observed(&db, || {
                crate::vector_read_memory::statement_child_proof::observe(
                    &db.declared_read_memory,
                    limit,
                    || entry(&db, historical, "SELECT FROM (", &HashMap::new(), snapshot),
                )
            });
            assert_eq!(
                allocation_count, 0,
                "parent refusal must precede ALL ordinary setup allocations"
            );
            borrowed_refusal(
                answer.unwrap_err(),
                "reader",
                "request_owner",
                child.size(),
                child.size() - 1,
                limit,
            );
            assert_eq!(
                (observation.capture().children, observation.capture().stages),
                (0, 0)
            );
            observation.assert_freed(&db);
            drop(observation);

            let limit = baseline + child.size() as u64 + keeper.size() as u64 - 1;
            let observation = Observation::begin(&db);
            let ((answer, _), _, allocation_count) = observed(&db, || {
                crate::vector_read_memory::statement_child_proof::observe(
                    &db.declared_read_memory,
                    limit,
                    || entry(&db, historical, "SELECT FROM (", &HashMap::new(), snapshot),
                )
            });
            assert_eq!(
                allocation_count, 1,
                "keeper admission refusal may allocate only the acquired native child"
            );
            borrowed_refusal(
                answer.unwrap_err(),
                "bounded_read",
                "native_adapter_keeper",
                keeper.size(),
                keeper.size() - 1,
                limit,
            );
            assert_eq!(
                (observation.capture().children, observation.capture().stages),
                (1, 1)
            );
            observation.assert_freed(&db);
            drop(observation);

            // Fault injection uses concrete layouts and the existing allocator's
            // null seam. std Arc OOM abort is deliberately outside these rows.
            for native_null in [true, false] {
                let observation = Observation::begin(&db);
                let (answer, _, allocation_count) = observed(&db, || {
                    crate::vector_read_memory::native_ownership_proof::arm_refuse_one_live_layout(
                        if native_null { child } else { keeper },
                    );
                    entry(&db, historical, "SELECT FROM (", &HashMap::new(), snapshot)
                });
                assert!(
                    !crate::vector_read_memory::native_ownership_proof::refusal_armed_for_test(),
                    "selected concrete null seam must actually be reached"
                );
                assert_eq!(
                    allocation_count,
                    usize::from(!native_null),
                    "a null native/keeper allocation must not enter ordinary setup"
                );
                let error = answer.unwrap_err();
                if native_null {
                    match error {
                        Error::NativeEngineOther(NativeRefusal::AllocatorFailed {
                            site,
                            layout,
                        }) => {
                            assert_eq!(site, "reader owner");
                            assert_eq!(layout, child);
                        }
                        other => panic!("native allocation error class changed: {other:?}"),
                    }
                    assert_eq!(observation.capture().children, 0);
                } else {
                    assert!(matches!(error, Error::Other(ref text)
                        if text == "cannot allocate admitted shared control"));
                    assert_eq!(observation.capture().children, 1);
                }
                observation.assert_freed(&db);
            }
        }
        assert_eq!(
            db.declared_read_memory.effective_limit_for_physical_test(),
            genuine_limit
        );
    }
}

// Acceptance: the last real trace holder outlives public execute conversion,
// slot removal, and caller unwind. The holder retains the same three original
// allocations until explicit release. No diagnostic getter acquires a child.
// Decisive mutant: retain a raw forwarding Arc without its external invoice.
#[test]
#[serial_test::serial]
fn an_escaped_actual_trace_keeps_the_native_child_and_original_invoice_alive() {
    let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_800_000_000_000);
    for file in [false, true] {
        let (_dir, db) = fixture(file);
        sql(
            &db,
            "CREATE TABLE points (id INT PRIMARY KEY, embedding VECTOR(3) SEARCH_MODE EXACT)",
        );
        db.execute(
            "INSERT INTO points VALUES (1, $v)",
            &HashMap::from([("v".into(), Value::Vector(vec![1.0, 0.0, 0.0]))]),
        )
        .unwrap();
        db.execute(
            "INSERT INTO points VALUES (2, $v)",
            &HashMap::from([("v".into(), Value::Vector(vec![0.0, 1.0, 0.0]))]),
        )
        .unwrap();
        let params = HashMap::from([("q".into(), Value::Vector(vec![1.0, 0.0, 0.0]))]);
        let query = "SELECT id FROM points ORDER BY embedding <=> $q USE VECTOR EXACT LIMIT 1";
        // Warm ordinary caches outside the observed window, then release its
        // actual trace rather than materializing a copied diagnostic trace.
        db.execute(query, &params).unwrap();
        db.clear_last_vector_search_trace();
        let snapshot = db.snapshot();
        for historical in [false, true] {
            for caller_panics in [false, true] {
                let observation = Observation::begin(&db);
                let (answer, _, _) =
                    observed(&db, || entry(&db, historical, query, &params, snapshot));
                let answer = answer.unwrap();
                assert_eq!(answer.rows(), vec![vec![Value::Int64(1)]]);
                let c = observation.assert_admitted();
                assert_eq!(
                    c.trace_stages, 2,
                    "both original trace allocations must be independently captured"
                );
                let retained = db
                    .last_vector_search_trace
                    .write()
                    .take()
                    .expect("real vector execution must publish an owning trace");
                assert!(retained.retained_credit_bytes_for_test() > 0);
                let invoice = retained.complete_invoice_for_test(|_| {});
                assert!(invoice.owner_control_invoice_present);
                assert!(invoice.admission_control_invoice_present);
                assert_eq!(
                    invoice.original_payer, c.forwarding_payload,
                    "the actual retained trace must keep the original ordinary forwarding payer"
                );
                assert_eq!(invoice.forwarding_payer, c.forwarding_payload);
                observation.assert_live();
                assert!(db.declared_read_memory.charged_bytes() > c.baseline);
                if caller_panics {
                    let panic = catch_unwind(AssertUnwindSafe(|| {
                        drop(answer);
                        panic!("caller after ordinary execute returned");
                    }));
                    assert_eq!(
                        panic.unwrap_err().downcast_ref::<&str>(),
                        Some(&"caller after ordinary execute returned")
                    );
                } else {
                    drop(answer);
                }
                observation.assert_live();
                assert!(db.last_vector_search_trace.read().is_none());
                assert_eq!(
                    retained.complete_invoice_for_test(|_| {}),
                    invoice,
                    "slot removal and caller unwind must not reconstruct the trace's invoice"
                );
                drop(retained);
                observation.assert_freed(&db);
            }
        }
    }
}

struct PanicPlugin {
    stage: AtomicUsize,
}
impl DatabasePlugin for PanicPlugin {
    fn on_query(&self, _: &str) -> Result<()> {
        if self.stage.load(Ordering::SeqCst) == 1 {
            panic!("producer on_query unwind");
        }
        Ok(())
    }
    fn post_query(&self, _: &str, _: std::time::Duration, _: &crate::plugin::QueryOutcome) {
        if self.stage.load(Ordering::SeqCst) == 2 {
            panic!("producer post_query unwind");
        }
    }
}

// Acceptance: callback unwind during construction (before execution and after
// an actual answer has been built) releases the original three controls once.
// This is distinct from a caller panicking after execute has returned.
// Decisive mutants: only success-path cleanup or refund adapter before Arc free.
#[test]
#[serial_test::serial]
fn ordinary_producer_callback_unwind_frees_each_original_control_once() {
    let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_800_000_000_000);
    for file in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let plugin = Arc::new(PanicPlugin {
            stage: AtomicUsize::new(0),
        });
        let db = if file {
            Database::open_with_options(
                dir.path().join("callback.db"),
                DatabaseOpenOptions {
                    plugin: plugin.clone(),
                    maintenance_policy: MaintenancePolicy::CallerDriven,
                    ..DatabaseOpenOptions::default()
                },
            )
            .unwrap()
        } else {
            Database::open_memory_with_plugin(plugin.clone()).unwrap()
        };
        sql(
            &db,
            "CREATE TABLE readings (id INT PRIMARY KEY, revision INT)",
        );
        sql(&db, "INSERT INTO readings VALUES (1, 10)");
        let query = "SELECT revision FROM readings";
        sql(&db, query); // cache setup outside the construction-unwind window
        let snapshot = db.snapshot();
        for historical in [false, true] {
            for stage in [1, 2] {
                plugin.stage.store(stage, Ordering::SeqCst);
                let observation = Observation::begin(&db);
                let (panic, _, _) = observed(&db, || {
                    catch_unwind(AssertUnwindSafe(|| {
                        entry(&db, historical, query, &HashMap::new(), snapshot)
                    }))
                });
                let marker = if stage == 1 {
                    "producer on_query unwind"
                } else {
                    "producer post_query unwind"
                };
                assert_eq!(
                    panic.unwrap_err().downcast_ref::<&str>(),
                    Some(&marker),
                    "fixture must unwind inside the selected real plugin callback"
                );
                observation.assert_admitted();
                observation.assert_freed(&db);
                drop(observation);
                plugin.stage.store(0, Ordering::SeqCst);
                assert_eq!(sql(&db, query).rows(), vec![vec![Value::Int64(10)]]);
            }
        }
    }
}

// A real ordinary execution supplies the positive allocator cut. Mutations
// affect only copied readonly receipts; no engine counter, free or invoice is
// reset, simulated, substituted or admitted by this diagnostic negative control.
#[test]
#[serial_test::serial]
fn ordinary_native_physical_cut_rejects_missing_duplicate_early_wrong_and_extra_receipts() {
    let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_800_000_000_000);
    for file in [false, true] {
        let (_dir, db) = fixture(file);
        sql(
            &db,
            "CREATE TABLE cut_rows (id INT PRIMARY KEY, embedding VECTOR(3) SEARCH_MODE EXACT)",
        );
        let params = HashMap::from([("q".into(), Value::Vector(vec![1.0, 0.0, 0.0]))]);
        db.execute("INSERT INTO cut_rows VALUES (1, $q)", &params)
            .unwrap();
        let query = "SELECT id FROM cut_rows ORDER BY embedding <=> $q USE VECTOR EXACT LIMIT 1";
        db.execute(query, &params).unwrap();
        db.clear_last_vector_search_trace();
        let observation = Observation::begin(&db);
        let (answer, _, _) = observed(&db, || db.execute(query, &params));
        let answer = answer.unwrap();
        assert_eq!(answer.rows(), vec![vec![Value::Int64(1)]]);
        drop(answer);
        db.clear_last_vector_search_trace();
        let c = observation.assert_admitted();
        observation.assert_freed(&db);
        let frees = oracle::source_free_for_test(c.stages + c.trace_stages);
        let live = oracle::source_live_at_free_for_test(c.stages + c.trace_stages);
        let valid = OriginalControlCut {
            request: c.trace_requests[1],
            original_payer: c.payer,
            invoice_payer: c.trace_invoices[1].0,
            invoice_bytes: c.trace_invoices[1].1,
            free: frees[4],
            independent_live: live[4],
            baseline: c.baseline as usize,
        };
        assert!(original_control_cut_is_complete(valid));
        let mut missing = valid;
        missing.free.2 = 0;
        let mut duplicate = valid;
        duplicate.free.2 = 2;
        let mut early = valid;
        early.free.0 -= 1;
        early.free.1 -= 1;
        let mut wrong_payer = valid;
        wrong_payer.invoice_payer ^= 1;
        let mut extra = valid;
        extra.free.0 += 1;
        extra.free.1 += 1;
        let mut wrong_layout = valid;
        wrong_layout.request.1 += 1;
        for cut in [missing, duplicate, early, wrong_payer, extra, wrong_layout] {
            assert!(!original_control_cut_is_complete(cut));
        }
    }
}

#[test]
#[serial_test::serial]
fn ordinary_answer_last_clone_preserves_actual_original_controls_through_physical_free() {
    let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_800_000_000_000);
    for file in [false, true] {
        let (_dir, db) = fixture(file);
        sql(
            &db,
            "CREATE TABLE answer_rows (id INT PRIMARY KEY, revision INT)",
        );
        sql(&db, "INSERT INTO answer_rows VALUES (1,10)");
        let old = db.snapshot();
        sql(&db, "UPDATE answer_rows SET revision = 20 WHERE id = 1");
        let new = db.snapshot();
        let query = "SELECT revision FROM answer_rows WHERE id = 1";
        sql(&db, query);
        for (historical, snapshot, expected) in [(false, new, 20), (true, old, 10), (true, new, 20)]
        {
            for original_first in [false, true] {
                let observation = Observation::begin(&db);
                let (answer, _, _) = observed(&db, || {
                    entry(&db, historical, query, &HashMap::new(), snapshot)
                });
                let answer = answer.unwrap();
                let c = observation.assert_admitted();
                assert_eq!(answer.rows(), vec![vec![Value::Int64(expected)]]);
                observation.assert_live();
                let clone = answer.clone();
                if original_first {
                    drop(answer);
                    observation.assert_live();
                    drop(clone);
                } else {
                    drop(clone);
                    observation.assert_live();
                    drop(answer);
                }
                observation.assert_freed(&db);
                let counters = oracle::source_free_for_test(3);
                eprintln!(
                    "actual_answer_last_clone file={file} historical={historical} original_first={original_first} requests={:?} counters={:?}",
                    c.requests,
                    &counters[..3]
                );
            }
        }
    }
}

#[test]
#[serial_test::serial]
fn retained_trace_and_live_answer_have_independent_actual_invoice_cuts() {
    let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_700_000_000_000);
    struct Cleanup;
    impl Drop for Cleanup {
        fn drop(&mut self) {
            ANSWER_TRACE.with(|slot| slot.set(AnswerTraceCapture::default()));
            oracle::disarm_source_free_watch();
        }
    }
    for file in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = if file {
            Database::open_with_options(
                dir.path().join("retained-trace.db"),
                DatabaseOpenOptions {
                    maintenance_policy: MaintenancePolicy::CallerDriven,
                    memory_limit: Some(64 * 1024 * 1024),
                    ..Default::default()
                },
            )
            .unwrap()
        } else {
            Database::open_memory()
        };
        // Match the original retained-trace fixture's schema, four rows, SQL,
        // binds, clock, and snapshot. Remove only its old setup trace before
        // this independent sample so unrelated retired owners cannot move it.
        sql(
            &db,
            "CREATE TABLE unpartitioned_items (id INT PRIMARY KEY, v VECTOR(4)) HISTORY ALL",
        );
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
        let params = HashMap::from([("v".into(), Value::Vector(vec![1.0, 0.0, 0.0, 0.0]))]);
        let query = "SELECT id FROM unpartitioned_items ORDER BY v <=> $v USE VECTOR EXACT LIMIT 1";
        db.clear_last_vector_search_trace();
        let parent = db.declared_read_memory_for_test();
        let limit = parent.effective_limit_for_physical_test();
        let counter = db.declared_read_memory.ordinary_parent_counter_for_test();
        let _cleanup = Cleanup;
        ANSWER_TRACE.with(|slot| {
            slot.set(AnswerTraceCapture {
                active: true,
                ..AnswerTraceCapture::default()
            })
        });
        let ((answer, allocations, count), sample) =
            crate::vector_read_memory::statement_child_proof::observe(&parent, limit, || {
                oracle::source_requests_for_test(counter, || {
                    db.execute_at_snapshot(query, &params, db.snapshot())
                })
            });
        let answer = answer.unwrap();
        let native = sample.native_controls.unwrap();
        let trace = ANSWER_TRACE.with(Cell::get);
        assert!(!trace.invalid);
        assert_eq!(trace.stages, 2);
        let native_requests = [
            native.block.actual_request.unwrap(),
            native.keeper.unwrap().actual_request.unwrap(),
            native.adapter.unwrap().actual_request.unwrap(),
        ];
        assert_eq!(native.keeper_payer, Some(native.owner));
        assert_eq!(native.adapter_payer, Some(native.owner));
        assert_eq!(native.keeper_credit_bytes, native_requests[1].1);
        assert_eq!(native.adapter_invoice_bytes, native_requests[2].1);
        for stage in 0..2 {
            assert_eq!(trace.invoices[stage].0, native.owner);
            assert_eq!(trace.invoices[stage].1, trace.requests[stage].1);
        }
        let components = answer.original_charge_components_for_test();
        assert_eq!(components[1], native.adapter.unwrap().payload);
        let mut publication = None;
        for request in &allocations[..count.min(allocations.len())] {
            if request.0 <= components[0] && components[0] < request.0.saturating_add(request.1) {
                assert!(
                    publication.replace(*request).is_none(),
                    "actual publication pointer is uniquely owned"
                );
            }
        }
        let publication =
            publication.expect("actual public carrier was allocated before execution");
        assert_eq!(publication.1, components[2]);
        let image_receipts = answer.image_control_receipts_for_test();
        let image_requests = image_receipts.map(|receipt| {
            let mut found = None;
            for request in &allocations[..count.min(allocations.len())] {
                if request.0 <= receipt.0 && receipt.0 < request.0.saturating_add(request.1) {
                    assert!(
                        found.replace(*request).is_none(),
                        "one physical allocation per retained image"
                    );
                }
            }
            let found = found.expect("the actual retained image was allocated before execution");
            if receipt.1 != 0 {
                assert_eq!(receipt.1, found.1);
            }
            found
        });
        assert_eq!(
            image_receipts[0].2, image_requests[1].0,
            "the actual retention-node invoice belongs to its image owner"
        );
        assert_eq!(
            image_receipts[1].2,
            native.adapter.unwrap().payload,
            "that image forwards to the same original native reader"
        );
        assert_eq!(components[4], image_receipts[0].3 + image_receipts[1].3);
        let requests = [
            native_requests[0],
            native_requests[1],
            native_requests[2],
            trace.requests[0],
            trace.requests[1],
            publication,
            image_requests[0],
            image_requests[1],
        ];
        oracle::watch_exact_frees_for_test(&requests, counter);
        let retained = db.last_vector_search_trace.write().take().unwrap();
        let invoice = retained.complete_invoice_for_test(|_| {});
        // Both answer and trace pin this same original child. The retained
        // trace remains its actual owner after the answer is physically freed.
        let before = unsafe { &*(native.counter as *const AtomicU64) }.load(Ordering::SeqCst);
        assert_eq!(sample.identity.target_last_held, before);
        let trace_due = native_requests[1..].iter().map(|r| r.1).sum::<usize>()
            + trace.requests.iter().map(|r| r.1).sum::<usize>()
            + invoice.prepaid_bytes
            + invoice.additional_bytes
            + invoice.score_credit_bytes
            + invoice.prepaid_control_bytes;
        drop(answer);
        let after = unsafe { &*(native.counter as *const AtomicU64) }.load(Ordering::SeqCst);
        let frees = oracle::source_free_for_test(8);
        let physical = frees[5];
        assert_eq!(physical.2, 1);
        assert_eq!(physical.0, physical.1);
        let due = sample.parent_before
            + native_requests[0].1 as u64
            + trace_due as u64
            + publication.1 as u64;
        assert_eq!(
            physical.0 as u64, due,
            "actual carrier stays prepaid at System.dealloc alongside the independent trace"
        );
        assert_eq!(
            after, trace_due as u64,
            "remaining live child debit equals the complete captured trace invoice and control requests"
        );
        let independent_answer_invoice = components[2]
            + components[3]
            + components[4]
            // The retention node's own invoice is already included in its
            // image's held payload; the image's physical block is external.
            + image_requests[1].1;
        assert_eq!(
            before - after,
            independent_answer_invoice as u64,
            "the released answer debit equals its payload invoices and independently captured physical controls"
        );
        let live = oracle::source_live_at_free_for_test(8);
        let trace_payload = invoice.prepaid_bytes
            + invoice.prepaid_control_bytes
            + invoice.additional_bytes
            + invoice.score_credit_bytes;
        for slot in 5..8 {
            assert_eq!(frees[slot].2, 1);
            assert_eq!(frees[slot].0, frees[slot].1);
            assert_eq!(
                frees[slot].0 as u64,
                sample.parent_before
                    + live[slot] as u64
                    + trace_payload as u64
                    + if slot == 6 {
                        (image_receipts[1].3 - image_receipts[0].1) as u64
                    } else {
                        0
                    },
                "each answer control physically frees while its own actual request and the independent live trace remain paid"
            );
        }
        assert!(
            frees[..5].iter().all(|free| free.2 == 0),
            "the trace still owns all five original controls"
        );
        drop(retained);
        let freed = oracle::source_free_for_test(8);
        let live = oracle::source_live_at_free_for_test(8);
        for i in 0..8 {
            assert_eq!(freed[i].2, 1);
            assert_eq!(freed[i].0, freed[i].1);
            if i < 5 {
                assert_eq!(freed[i].0 as u64, sample.parent_before + live[i] as u64);
            }
        }
        assert_eq!(
            db.declared_read_memory.charged_bytes(),
            sample.parent_before
        );
        oracle::disarm_source_free_watch();
        ANSWER_TRACE.with(|slot| slot.set(AnswerTraceCapture::default()));
        // A separate companion to the protected stale-baseline fixture: the
        // preceding named answer and trace have now physically freed every
        // request. Its exact actual native-block request determines this cap.
        let stable = sample.parent_before;
        let refusal_limit = stable + native_requests[0].1 as u64;
        let ((refused, _, _), refusal) = crate::vector_read_memory::statement_child_proof::observe(
            &parent,
            refusal_limit,
            || {
                oracle::source_requests_for_test(counter, || {
                    db.execute_at_snapshot(query, &params, db.snapshot())
                })
            },
        );
        let error = refused.unwrap_err();
        let Error::MemoryBudgetExceeded {
            requested_bytes,
            available_bytes,
            budget_limit_bytes,
            operation,
            ..
        } = error
        else {
            panic!("the actual stable-baseline query must retain its typed refusal: {error:?}");
        };
        let actual = refusal
            .identity
            .last_target_refusal
            .expect("actual failed request must be recorded before later frees");
        assert_eq!(actual.kind, "ChildRefused");
        assert_eq!(actual.operation, "native_adapter_keeper");
        assert_eq!(actual.bytes, native_requests[1].1);
        assert_eq!(requested_bytes, actual.bytes);
        assert_eq!(available_bytes, 0);
        assert_eq!(budget_limit_bytes, refusal_limit as usize);
        assert_eq!(operation, actual.operation);
        assert_eq!(refusal.children, 1);
        assert_eq!(refusal.refusals, 1);
        assert_eq!(refusal.peak, native_requests[0].1 as u64);
        assert_eq!(refusal.control_bytes, native_requests[0].1);
        // A file-backed fixture can retain a separate setup owner. Its
        // independently captured initial control set is unchanged by freeing
        // all eight requests above; never count it as this query's dead trace.
        let first_created = sample
            .identity
            .target_created
            .expect("actual first constructor");
        assert_eq!(first_created.child_control_bytes, native_requests[0].1);
        let standing_controls = first_created
            .live_controls_after
            .checked_sub(native_requests[0].1 as u64)
            .unwrap();
        assert_eq!(
            actual.live_controls_before,
            standing_controls + native_requests[0].1 as u64,
            "only the preexisting fixture controls and the new actual native block remain"
        );
        assert_eq!(refusal.parent_before, stable);
        assert_eq!(refusal.parent_after, stable);
        assert_eq!(refusal.final_bytes, 0);
        assert_eq!(refusal.drops, 1);
        assert_eq!(refusal.parent_funding_mismatches, 0);
        eprintln!(
            "actual_stable_refusal_companion file={file} freed_trace_owner={} trace_plus_native_control={} stable={stable} cap={refusal_limit} refused={actual:?}",
            native.owner,
            trace_due + native_requests[0].1
        );
        eprintln!(
            "actual_answer_trace_cuts file={file} sampled_return={before} after_answer_drop={after} released_by_answer={} actual_components={components:?} publication={publication:?} images={image_requests:?} image_receipts={image_receipts:?} independent_answer_invoice={independent_answer_invoice} physical={physical:?} trace_invoice={invoice:?}",
            before - after
        );
    }
}

#[test]
#[serial_test::serial]
fn actual_request_and_cursor_traces_keep_the_same_original_physical_invoices() {
    let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_700_000_000_000);
    for file in [false, true] {
        for route in ["request", "session", "cursor"] {
            let (_dir, db) = fixture(file);
            sql(
                &db,
                "CREATE TABLE route_points (id INT PRIMARY KEY, v VECTOR(4)) HISTORY ALL",
            );
            for id in 0..4 {
                db.execute(
                    "INSERT INTO route_points VALUES ($id,$v)",
                    &HashMap::from([
                        ("id".into(), Value::Int64(id)),
                        ("v".into(), Value::Vector(vec![1.0, id as f32, 0.0, 0.0])),
                    ]),
                )
                .unwrap();
            }
            db.clear_last_vector_search_trace();
            let query = "SELECT id FROM route_points ORDER BY v <=> $v USE VECTOR EXACT LIMIT 1";
            let params = HashMap::from([("v".into(), Value::Vector(vec![1.0, 0.0, 0.0, 0.0]))]);
            let mut limits = contextdb_core::read_contract::ReadLimits::default();
            if route == "cursor" {
                limits.cursor_page_rows = 1;
            }
            let session = db.read_session(limits).unwrap();
            if route == "cursor" {
                // Initialize the real standing expiry owner as fixture setup.
                // It belongs to this live session, not to the vector operation.
                let ready = session
                    .open_cursor("SELECT id FROM route_points WHERE id < 0", &HashMap::new())
                    .unwrap();
                assert!(ready.first_page().rows().is_empty());
                ready.close().unwrap();
            }
            let capture = Observation::begin_at_actual_native_constructor(&db);
            let mut answer = None;
            let mut cursor = None;
            let mut page = None;
            let mut initial_page = None;
            let _ = observed(&db, || {
                match route {
                    "request" => {
                        // Exercise the real provider's Native route and its
                        // explicit refund propagation, including real SQL.
                        let request = db
                            .declared_read_request_provider()
                            .request_budget_owner()
                            .unwrap();
                        answer = Some(
                            crate::executor::bounded::with_request_read_memory(
                                request,
                                |error| error,
                                |owner, refund| {
                                    db.execute_owned_with_native_trace_refund(
                                        query,
                                        &params,
                                        owner,
                                        refund.as_ref(),
                                    )
                                    .and_then(OwnedQueryResult::into_public)
                                },
                            )
                            .unwrap(),
                        );
                    }
                    "session" => answer = Some(session.execute(query, &params).unwrap()),
                    "cursor" => {
                        let mut opened = session.open_cursor(
                            "SELECT id FROM route_points ORDER BY v <=> $v USE VECTOR EXACT LIMIT 2", &params).unwrap();
                        let first = opened.first_page().clone();
                        assert_eq!(first.rows(), vec![vec![Value::Int64(0)]]);
                        assert!(first.has_more());
                        let next = opened.fetch(Some(std::num::NonZeroUsize::MIN)).unwrap();
                        assert_eq!(next.rows(), vec![vec![Value::Int64(1)]]);
                        assert!(!next.has_more());
                        initial_page = Some(first);
                        page = Some(next);
                        cursor = Some(opened);
                    }
                    _ => unreachable!(),
                }
            });
            if let Some(answer) = &answer {
                assert_eq!(answer.rows(), vec![vec![Value::Int64(0)]]);
            }
            if let Some(page) = &page {
                assert_eq!(page.rows(), vec![vec![Value::Int64(1)]]);
            }
            assert_route_original_physical_invoices(&db, capture, route, file, || {
                drop(answer);
                drop(page);
                drop(initial_page);
                if let Some(cursor) = cursor {
                    cursor.close().unwrap();
                }
                (route == "cursor").then_some(session)
            });
        }
    }
}

fn assert_route_original_physical_invoices(
    db: &Database,
    capture: Observation,
    route: &str,
    file: bool,
    cleanup: impl FnOnce() -> Option<crate::ReadSession>,
) {
    let controls = capture.assert_admitted();
    assert_eq!(
        controls.trace_stages, 2,
        "the actual {route} vector trace must reach both native factories"
    );
    capture.assert_live();
    let retained = db.last_vector_search_trace.write().take().unwrap();
    let invoice = retained.complete_invoice_for_test(|_| {});
    assert_eq!(invoice.original_payer, controls.forwarding_payload);
    let mut requests = [(0, 0, 0); 16];
    requests[..3].copy_from_slice(&controls.requests);
    requests[3..5].copy_from_slice(&controls.trace_requests);
    let mut count = 5;
    for request in invoice.allocations {
        if request.1 != 0 {
            requests[count] = request;
            count += 1;
        }
    }
    requests[count] = invoice.prepaid_control;
    count += 1;
    oracle::watch_exact_frees_for_test(&requests[..count], controls.parent_counter);
    let session = cleanup();
    // The escaped actual trace now keeps its own original invoices.
    assert_eq!(oracle::source_free_for_test(count)[3].2, 0);
    assert_eq!(oracle::source_free_for_test(count)[4].2, 0);
    drop(retained);
    let freed = oracle::source_free_for_test(count);
    let live = oracle::source_live_at_free_for_test(count);
    for slot in 0..count {
        assert_eq!(
            freed[slot].2, 1,
            "{route} physical request {slot} frees once"
        );
        assert_eq!(freed[slot].0, freed[slot].1);
        let token = count - 1;
        let payload_group = slot >= 5 && slot < token;
        let extra = if payload_group {
            // These buffers share one prepaid invoice. Already freed
            // buffers stay charged until the entire payload retires.
            let all_payload = requests[5..token].iter().map(|r| r.1).sum::<usize>();
            assert_eq!(all_payload, invoice.prepaid_bytes);
            let controls_live = requests[..5].iter().map(|r| r.1).sum::<usize>();
            controls_live
                + invoice.prepaid_bytes
                + invoice.prepaid_control_bytes
                + invoice.additional_bytes
                + invoice.score_credit_bytes
        } else {
            live[slot]
                + if slot == token {
                    invoice.additional_bytes
                } else {
                    0
                }
        };
        assert_eq!(
            freed[slot].0,
            controls.baseline as usize + extra,
            "{route} keeps each actual original invoice through physical free, including the grouped payload and inline credits"
        );
    }
    assert_eq!(db.declared_read_memory.charged_bytes(), controls.baseline);
    eprintln!(
        "actual_request_cursor_physical route={route} file={file} requests={:?} frees={:?}",
        &requests[..count],
        &freed[..count]
    );
    drop(session);
}

struct DirectPhysicalClock;
impl contextdb_core::read_contract::DeadlineClock for DirectPhysicalClock {
    fn now_ms(&self) -> u64 {
        0
    }
    fn wait_until(&self, _: u64) -> contextdb_core::read_contract::DeadlineWait<'_> {
        Box::pin(std::future::pending())
    }
}

#[test]
#[serial_test::serial]
fn actual_direct_file_public_readers_keep_original_invoices_through_physical_free() {
    use crate::direct_file_reader::{DirectFileReader, DirectReaderConfig};
    use std::num::NonZeroUsize;
    let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_700_000_000_000);
    for route in [
        "direct-reader-execute",
        "direct-reader-cursor",
        "direct-session-execute",
    ] {
        let (dir, writer) = fixture(true);
        sql(
            &writer,
            "CREATE TABLE route_points (id INT PRIMARY KEY, v VECTOR(4)) HISTORY ALL",
        );
        for id in 0..4 {
            writer
                .execute(
                    "INSERT INTO route_points VALUES ($id,$v)",
                    &HashMap::from([
                        ("id".into(), Value::Int64(id)),
                        ("v".into(), Value::Vector(vec![1.0, id as f32, 0.0, 0.0])),
                    ]),
                )
                .unwrap();
        }
        writer.close().unwrap();
        drop(writer);
        let path = dir.path().join("ordinary.db");
        let params = HashMap::from([("v".into(), Value::Vector(vec![1.0, 0.0, 0.0, 0.0]))]);
        let query = "SELECT id FROM route_points ORDER BY v <=> $v USE VECTOR EXACT LIMIT 1";
        if route == "direct-session-execute" {
            let session =
                crate::ReadSession::open_with_options(&path, crate::ReadSessionOptions::default())
                    .unwrap();
            assert!(
                session.file_snapshot_for_test().is_some(),
                "must be the actual committed direct-file route"
            );
            session
                .__with_resident_history_for_test(|db| {
                    db.clear_last_vector_search_trace();
                    let capture = Observation::begin_at_actual_native_constructor(db);
                    let (answer, _, _) = observed(db, || session.execute(query, &params).unwrap());
                    assert_eq!(answer.rows(), vec![vec![Value::Int64(0)]]);
                    // The live session owns only its standing root, not this
                    // per-operation native child or the escaped actual trace.
                    assert_route_original_physical_invoices(db, capture, route, true, || {
                        drop(answer);
                        None
                    });
                    Ok(())
                })
                .unwrap();
        } else {
            let config = DirectReaderConfig::new(
                contextdb_core::read_contract::ReadLimits::default(),
                Arc::new(DirectPhysicalClock),
            );
            let reader = DirectFileReader::open(&path, config).unwrap();
            let target = reader.execution_target();
            let db = target.resident_history_database_for_test().unwrap();
            db.clear_last_vector_search_trace();
            let capture = Observation::begin_at_actual_native_constructor(db);
            if route == "direct-reader-execute" {
                let (answer, _, _) = observed(db, || reader.execute(query, &params).unwrap());
                assert_eq!(answer.result.rows(), vec![vec![Value::Int64(0)]]);
                assert_route_original_physical_invoices(db, capture, route, true, || {
                    drop(answer);
                    None
                });
            } else {
                let (mut opened, _, _) = observed(db, || {
                    reader.open_cursor(
                    "SELECT id FROM route_points ORDER BY v <=> $v USE VECTOR EXACT LIMIT 2",
                    &params, NonZeroUsize::MIN).unwrap()
                });
                assert_eq!(
                    opened.first_page.page_for_test().rows,
                    &[vec![Value::Int64(0)]]
                );
                assert!(opened.first_page.page_for_test().has_more);
                let next = opened.cursor.fetch(Some(NonZeroUsize::MIN)).unwrap();
                assert_eq!(next.page_for_test().rows, &[vec![Value::Int64(1)]]);
                assert!(!next.page_for_test().has_more);
                assert_route_original_physical_invoices(db, capture, route, true, || {
                    drop(next);
                    drop(opened.first_page);
                    opened.cursor.close().unwrap();
                    drop(opened.cursor);
                    None
                });
            }
        }
    }
}
