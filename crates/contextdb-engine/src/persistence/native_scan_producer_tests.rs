//! Independent production-scan proofs. Harness owners/threads exist before the
//! allocator windows. All byte expectations derive from concrete Layout values.
use super::*;
use crate::persistence::native_storage_admission::StorageAdmissionScope;
use crate::vector_read_memory::{DeclaredReadMemory, DeclaredReadRoot, declared_child_observer};
use redb::ScanAllowance;
use std::sync::{Arc, Barrier};

// Extend the inherited allocator's actual injected-null cut with scalar-only
// evidence. This samples the executing child's real debit, never a test budget.
thread_local! {
    static NULL_REQUEST: std::cell::Cell<(usize, usize, usize)> = const { std::cell::Cell::new((0,0,0)) };
}
pub(super) fn allocator_null(layout: Layout) {
    let counter = SOURCE_REQUEST_COUNTER
        .try_with(std::cell::Cell::get)
        .unwrap_or(0);
    if counter != 0 {
        NULL_REQUEST.with(|slot| {
            slot.set((
                layout.size(),
                layout.align(),
                observed_source_funding(counter),
            ))
        });
    }
}

type Request = (usize, usize, usize);
struct Reader {
    parent: DeclaredReadRoot,
    child: contextdb_core::read_memory::ReadMemoryOwner,
    scope: StorageAdmissionScope,
    counter: usize,
}
impl Reader {
    fn new(limit: u64) -> Self {
        let parent =
            DeclaredReadMemory::from_reader_limit(limit).expect("admit the declared fixture root");
        let (child, _) =
            crate::vector_read_memory::statement_child_proof::observe(&parent, limit, || {
                parent.child().unwrap()
            });
        let counter = declared_child_observer(&child)
            .unwrap()
            .physical_funding_counter_for_test();
        let scope = StorageAdmissionScope::reader_operation(child.clone()).unwrap();
        Self {
            parent,
            child,
            scope,
            counter,
        }
    }
    fn used(&self) -> usize {
        unsafe { &*(self.counter as *const std::sync::atomic::AtomicU64) }
            .load(AtomicOrdering::SeqCst) as usize
    }
    fn finish(self) {
        let Self {
            parent,
            child,
            scope,
            ..
        } = self;
        drop(scope);
        drop(child);
        assert_eq!(
            parent.retained_bytes_for_test(),
            crate::vector_read_memory::native_control_layouts_for_test()
                .0
                .size() as u64
        );
        parent.assert_final_root_free_for_test();
    }
    fn allowance(&self) -> ScanAllowance {
        redb::with_native_storage_admission(self.scope.retain_admission(), || {
            ScanAllowance::try_new_native()
        })
        .unwrap()
    }
}
// The inherited free-watch reads AtomicUsize; its ABI is the actual desktop's
// u64 reader counter. Do not silently certify that watch on 32-bit platforms.
const _: () = assert!(std::mem::size_of::<usize>() == std::mem::size_of::<u64>());

fn capture<T>(
    reader: &Reader,
    action: impl FnOnce() -> T,
) -> (T, [Request; 256], usize, [usize; 256]) {
    let value = reader_requests_for_test(reader.counter, action);
    (
        value,
        NATIVE_REQUESTS.with(std::cell::Cell::get),
        NATIVE_REQUEST_COUNT.with(std::cell::Cell::get),
        source_request_credit_for_test(),
    )
}
fn pair(reader: &Reader, requests: &[Request], identity: (usize, Layout, usize), credit: &[usize]) {
    assert_eq!(
        requests.len(),
        2,
        "one original receipt and its actual target only"
    );
    let (pointer, layout, receipt) = identity;
    let receipt_layout = StorageAdmissionScope::receipt_layout_for_scan_test();
    assert_eq!(
        requests[0],
        (receipt, receipt_layout.size(), receipt_layout.align())
    );
    assert_eq!(requests[1], (pointer, layout.size(), layout.align()));
    assert_ne!(pointer, receipt);
    let total = layout.size() + receipt_layout.size();
    assert_eq!(
        &credit[..2],
        &[total, total],
        "BOTH exact layouts are paid by the original child before either allocation"
    );
    assert!(reader.used() >= total);
    assert_eq!(source_unfunded_peak_for_test().0, 0);
}
struct Watches;
impl Drop for Watches {
    fn drop(&mut self) {
        disarm_source_free_watch();
    }
}
fn freed_pair(requests: &[Request], before: usize) {
    let free = source_free_for_test(2);
    // The target frees first, then the receipt frees while BOTH of its invoices
    // still exist. A same-layout stranger cannot satisfy the watched pointers.
    assert_eq!(free[1], (before, before, 1));
    assert_eq!(
        free[0],
        (before, before, 1),
        "receipt releases target and receipt invoices only AFTER its physical free"
    );
    assert_eq!(
        NATIVE_FREE[0].bytes.load(AtomicOrdering::SeqCst),
        requests[0].1
    );
    assert_eq!(
        NATIVE_FREE[1].bytes.load(AtomicOrdering::SeqCst),
        requests[1].1
    );
}

// A native scan value is an inline clone of the original admission: building
// or cloning it allocates nothing and debits nothing. Every native entry
// still prepays its own node and receipt from that original payer.
fn construct_with_no_allocation(reader: &Reader) -> ScanAllowance {
    let before = reader.used();
    let (allowance, _, count, _) = capture(reader, || reader.allowance());
    assert_eq!(count, 0, "a native scan value allocates nothing");
    assert_eq!(reader.used(), before, "and adds no debit");
    let (clone, _, count, _) = capture(reader, || allowance.clone());
    assert_eq!(count, 0, "a clone of it allocates nothing");
    assert_eq!(reader.used(), before, "and adds no debit");
    drop(clone);
    allowance
}

/// The reader's own scope control: the original admission every native scan
/// value and node retains.
fn scope_control(reader: &Reader) -> Request {
    let (pointer, layout) = reader.scope.allocation_observation_for_scan_test();
    (pointer, layout.size(), layout.align())
}

/// Drop the reader's own scope and child and prove the declared root ends
/// with only its control, physically free.
fn finish_parts(parent: DeclaredReadRoot, child: contextdb_core::read_memory::ReadMemoryOwner) {
    drop(child);
    assert_eq!(
        parent.retained_bytes_for_test(),
        crate::vector_read_memory::native_control_layouts_for_test()
            .0
            .size() as u64
    );
    parent.assert_final_root_free_for_test();
}

// Separate test: exact physical ownership cannot be a row of a cache-behavior
// test; decisive mutants substitute an equal-sized payer or refund before free.
#[test]
#[serial]
fn native_scan_value_allocates_nothing_and_its_node_keeps_original_invoices_through_last_physical_free()
 {
    let reader = Reader::new(1 << 20);
    let standing = reader.used();
    let allowance = construct_with_no_allocation(&reader);
    let decoy = Reader::new(1 << 20);
    let decoy_standing = decoy.used();
    let (guard, node_requests, count, credit) = capture(&reader, || {
        redb::with_native_storage_admission(decoy.scope.retain_admission(), || {
            allowance.try_enter_native()
        })
        .unwrap()
    });
    assert_eq!(
        decoy.used(),
        decoy_standing,
        "entry retains the original payer despite a different current admission"
    );
    decoy.finish();
    pair(
        &reader,
        &node_requests[..count],
        guard.allocation_observation_for_test(),
        &credit,
    );
    let node_cost = reader.used() - standing;
    assert_eq!(
        node_cost,
        ScanAllowance::node_layout_for_test().size()
            + StorageAdmissionScope::receipt_layout_for_scan_test().size(),
        "the node and its receipt are the only debit of a native scan entry"
    );
    watch_source_free_for_test(&node_requests[..count], reader.counter);
    let _watches = Watches;
    let before = reader.used();
    let helper = redb::scan_reads();
    drop(guard); // current-node helper is now its sole guard
    assert_eq!(source_free_for_test(2)[0].2, 0);
    assert_eq!(source_free_for_test(2)[1].2, 0);
    assert_eq!(reader.used(), before);
    drop(helper);
    freed_pair(&node_requests[..count], before);
    assert_eq!(reader.used(), standing);
    assert_eq!(ScanAllowance::current_node_for_test(), 0);
    // The original admission control stays paid and allocated while any
    // captured holder lives, and is physically freed under its own invoice
    // only when the last one drops.
    watch_exact_frees_for_test(&[scope_control(&reader)], reader.counter);
    let last = allowance.clone();
    let Reader {
        parent,
        child,
        scope,
        counter,
    } = reader;
    let used = || {
        unsafe { &*(counter as *const std::sync::atomic::AtomicU64) }.load(AtomicOrdering::SeqCst)
            as usize
    };
    drop(scope);
    drop(allowance);
    assert_eq!(
        source_free_for_test(1)[0].2,
        0,
        "a captured holder keeps the original admission control allocated"
    );
    assert_eq!(used(), standing, "and its invoice in place");
    drop(last);
    assert_eq!(
        source_free_for_test(1)[0],
        (standing, standing, 1),
        "the last captured holder frees the control before its invoice settles"
    );
    assert_eq!(used(), 0);
    disarm_source_free_watch();
    finish_parts(parent, child);
}

// A distinct proof of preallocation refusal/actual null. No fake callback or
// post-failure String/Box allocation is allowed to pass as a native refusal.
#[test]
#[serial]
fn native_scan_value_needs_only_an_admission_and_entry_refusals_distinguish_node_and_receipt() {
    let node_layout = ScanAllowance::node_layout_for_test();
    let receipt = StorageAdmissionScope::receipt_layout_for_scan_test();
    let roomy = Reader::new(1 << 20);
    let parent_standing = roomy.parent.retained_bytes_for_test();
    roomy.finish();
    // With a valid admission and not one byte of headroom, the value still
    // builds; without any admission it refuses, and neither path allocates.
    {
        let reader = Reader::new(parent_standing);
        drop(construct_with_no_allocation(&reader));
        let (refused, _, count, _) = capture(&reader, ScanAllowance::try_new_native);
        assert!(matches!(
            refused,
            Err(redb::StorageError::Native(
                redb::NativeColdReadError::AdmissionRefused
            ))
        ));
        assert_eq!(
            count, 0,
            "a missing admission refuses before any allocation"
        );
        reader.finish();
    }
    for receipt_denied in [false, true] {
        // Leave exactly one byte less than the node, or than the node plus its
        // independent receipt.
        let extra = node_layout.size() + if receipt_denied { receipt.size() } else { 0 };
        let reader = Reader::new(parent_standing + (extra - 1) as u64);
        let allowance = construct_with_no_allocation(&reader);
        let before = reader.used();
        let (result, _, count, _) = capture(&reader, || allowance.try_enter_native().map(drop));
        assert!(matches!(
            result,
            Err(redb::StorageError::Native(
                redb::NativeColdReadError::AdmissionRefusedAt(_)
            ))
        ));
        assert_eq!(count, 0, "deny before receipt AND node allocation");
        assert_eq!(reader.used(), before, "partial reservation rolls back");
        assert_eq!(ScanAllowance::current_node_for_test(), 0);
        drop(result);
        drop(allowance);
        reader.finish();
    }
    for receipt_null in [false, true] {
        let reader = Reader::new(1 << 20);
        let allowance = construct_with_no_allocation(&reader);
        // The existing allocator injects null BEFORE System, at the actual
        // selected producer request, never by manufacturing an error value.
        let refused = if receipt_null { receipt } else { node_layout };
        NULL_REQUEST.with(|slot| slot.set((0, 0, 0)));
        crate::vector_read_memory::native_ownership_proof::arm_refuse_one_live_layout(refused);
        let before = reader.used();
        let _watches = Watches;
        if !receipt_null {
            watch_next_exact_free_for_test(receipt, reader.counter);
        }
        let (result, requests, count, _) =
            capture(&reader, || allowance.try_enter_native().map(drop));
        assert!(!crate::vector_read_memory::native_ownership_proof::refusal_armed_for_test());
        assert_eq!(
            NULL_REQUEST.with(std::cell::Cell::get),
            (
                refused.size(),
                refused.align(),
                node_layout.size() + receipt.size()
            ),
            "the actual null request observes both original child invoices already paid"
        );
        if receipt_null {
            assert!(matches!(&result, Err(redb::StorageError::Native(
                redb::NativeColdReadError::AdmissionRefusedAt(owner))) if owner.inspect_failure(|failure|
                    failure.and_then(|e| e.downcast_ref::<crate::persistence::native_storage_admission::NativeStorageFailure>())
                        .is_some_and(|e| matches!(e,
                            crate::persistence::native_storage_admission::NativeStorageFailure::AllocatorFailed { layout, .. } if *layout == receipt)))));
            assert_eq!(count, 0);
        } else {
            assert!(matches!(&result, Err(redb::StorageError::Native(
                redb::NativeColdReadError::AllocatorFailed { layout, .. })) if *layout == node_layout));
            assert_eq!(
                count, 1,
                "only the authentic receipt was allocated then freed"
            );
            assert_eq!(
                (requests[0].1, requests[0].2),
                (receipt.size(), receipt.align())
            );
            let paid = before + node_layout.size() + receipt.size();
            assert_eq!(
                source_free_for_test(1)[0],
                (paid, paid, 1),
                "actual failed node releases its real receipt only after its physical free"
            );
        }
        disarm_source_free_watch();
        assert_eq!(reader.used(), before);
        assert_eq!(ScanAllowance::current_node_for_test(), 0);
        drop(result);
        drop(allowance);
        reader.finish();
    }
}

// A/B/A creates three distinct entries; current-node helpers must NOT move or
// allocate nodes. All six unlink permutations exercise interior and tail cuts.
// Native entries draw on no fixed growth allowance: the shared cache's own
// capacity bounds their working set, proved by its separate guards.
#[test]
#[serial]
fn native_scan_nested_helpers_and_every_drop_order_retain_the_original_admission() {
    std::thread::spawn(|| {
        let entry_cost = ScanAllowance::node_layout_for_test().size()
            + StorageAdmissionScope::receipt_layout_for_scan_test().size();
        for order in [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ] {
            let reader = Reader::new(1 << 20);
            let standing = reader.used();
            let a = construct_with_no_allocation(&reader);
            let b = construct_with_no_allocation(&reader);
            let mut guards = [None, None, None];
            for (index, allowance) in [&a, &b, &a].into_iter().enumerate() {
                let (guard, requests, count, credit) =
                    capture(&reader, || allowance.try_enter_native().unwrap());
                pair(
                    &reader,
                    &requests[..count],
                    guard.allocation_observation_for_test(),
                    &credit,
                );
                guards[index] = Some(guard);
            }
            let nodes = guards
                .each_ref()
                .map(|g| g.as_ref().unwrap().allocation_observation_for_test().0);
            assert_ne!(nodes[0], nodes[1]);
            assert_ne!(nodes[0], nodes[2]);
            assert_ne!(nodes[1], nodes[2]);
            assert_eq!(reader.used(), standing + 3 * entry_cost);
            let before = reader.used();
            let ((helper, ()), _, count, _) = capture(&reader, || {
                (
                    redb::scan_reads(),
                    redb::with_scan_reads(|| {
                        redb::with_scan_reads(|| {
                            assert_eq!(ScanAllowance::current_node_for_test(), nodes[2]);
                            assert!(
                                !ScanAllowance::take_current_growth_for_test(1),
                                "a native entry draws on no fixed growth allowance"
                            );
                        })
                    }),
                )
            });
            assert_eq!(
                count, 0,
                "ordinary nested redb body helpers allocate no scan controls"
            );
            assert_eq!(reader.used(), before);
            drop(helper);
            for (dropped, index) in order.into_iter().enumerate() {
                drop(guards[index].take());
                let expected = (0..3)
                    .rev()
                    .find(|i| guards[*i].is_some())
                    .map_or(0, |i| nodes[i]);
                assert_eq!(
                    ScanAllowance::current_node_for_test(),
                    expected,
                    "the innermost live node is current again"
                );
                assert!(!ScanAllowance::take_current_growth_for_test(1));
                assert_eq!(
                    reader.used(),
                    standing + (2 - dropped) * entry_cost,
                    "each unlinked entry returns exactly its own node and receipt to the original payer"
                );
            }
            assert_eq!(ScanAllowance::current_node_for_test(), 0);
            let (resumed, requests, count, _) = capture(&reader, || {
                let resumed = a.try_enter_native().unwrap();
                assert_eq!(reader.used(), standing + entry_cost);
                assert!(!ScanAllowance::take_current_growth_for_test(1));
                resumed.allocation_observation_for_test()
            });
            assert_eq!(
                count, 2,
                "one receipt/node only, no retained TLS Vec backing"
            );
            assert_eq!(
                (requests[0].0, requests[1].0),
                (resumed.2, resumed.0),
                "the resumed entry allocates its own fresh receipt and node"
            );
            assert_eq!(reader.used(), standing);
            drop((a, b));
            reader.finish();
        }
        // A continuous native interval includes guard destruction as well as
        // entry: an uncharged TLS buffer created/retained during cleanup must
        // fail the physical live-byte census, even if all invoices refund.
        let reader = Reader::new(1 << 20);
        let a = construct_with_no_allocation(&reader);
        let b = construct_with_no_allocation(&reader);
        let before = reader.used();
        let ((), _, count) = native_requests_for_test(|| {
            let outer = a.try_enter_native().unwrap();
            let middle = b.try_enter_native().unwrap();
            let inner = a.try_enter_native().unwrap();
            let helper = redb::scan_reads();
            drop(outer);
            drop(inner);
            drop(middle);
            drop(helper);
        });
        assert_eq!(
            count, 6,
            "only three actual node/receipt pairs in the entire interval"
        );
        {
            let measured = MEASUREMENT.lock().unwrap();
            assert_eq!(
                measured.payload_live, 0,
                "no scan control or TLS backing survives the final guard"
            );
            assert_eq!(
                measured.live, 0,
                "including every oracle-wrapped allocation"
            );
        }
        assert_eq!(reader.used(), before);
        assert_eq!(ScanAllowance::current_node_for_test(), 0);
        drop((a, b));
        reader.finish();
    })
    .join()
    .unwrap();
}

// Genuine scope unwinding uses a preallocated panic payload, so allocating the
// application's payload is outside the native scan interval.
#[test]
#[serial]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn native_scan_error_and_unwind_restore_scope_and_physically_free_entries() {
    for unwind in [false, true] {
        let reader = Reader::new(1 << 20);
        let parent = reader.parent.clone();
        let child = reader.child.clone(); // keeps ONLY the sampled child alive
        let child_control = parent.retained_bytes_for_test() as usize - reader.used();
        let scope = scope_control(&reader);
        let scope_cost = scope.1;
        assert_eq!(reader.used(), scope_cost);
        let allowance = construct_with_no_allocation(&reader);
        let (guard, node_requests, count, credit) =
            capture(&reader, || allowance.try_enter_native().unwrap());
        pair(
            &reader,
            &node_requests[..count],
            guard.allocation_observation_for_test(),
            &credit,
        );
        let all = [scope, node_requests[0], node_requests[1]];
        // Scope was created outside the measured window. Its exact pointer and
        // actual allocator header still bind its physical free, including window 0.
        watch_exact_frees_for_test(&all, reader.counter);
        let _watches = Watches;
        let payload: Box<dyn std::any::Any + Send> = Box::new(71usize);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            // These actual owners die inside the catch boundary on BOTH paths.
            let reader = reader;
            reader.scope.run_native(|| {
                let _guard = guard;
                let _allowance = allowance;
                let _helper = redb::scan_reads();
                if unwind {
                    std::panic::resume_unwind(payload);
                }
                Err::<(), _>(contextdb_core::Error::Other(
                    "expected source failure".into(),
                ))
            })
        }));
        assert_eq!(result.is_err(), unwind);
        if let Ok(result) = result {
            assert!(result.is_err());
        }
        assert!(!redb::native_storage_active());
        assert_eq!(ScanAllowance::current_node_for_test(), 0);
        let free = source_free_for_test(3);
        let node_cost = all[1].1 + all[2].1;
        // The node frees first and its receipt follows while both of its
        // invoices remain; the original admission control frees last, under
        // its own invoice, once no captured holder remains.
        let peak = scope_cost + node_cost;
        assert_eq!(free[2], (peak, peak, 1));
        assert_eq!(free[1], (peak, peak, 1));
        assert_eq!(free[0], (scope_cost, scope_cost, 1));
        assert_eq!(parent.retained_bytes_for_test() as usize, child_control);
        disarm_source_free_watch();
        finish_parts(parent, child);
    }
}

// Native guards stay on their creating threads; the continuation's shared
// allowance moves only after the earlier entry is gone. No application thread
// setup allocation falls inside either node measurement window.
#[test]
#[serial]
fn native_scan_threads_share_one_captured_admission_and_resume_after_first_thread_entry_drops() {
    for first in [0, 1] {
        let reader = Reader::new(1 << 20);
        let allowance = construct_with_no_allocation(&reader);
        let standing = reader.used();
        let start = Arc::new(Barrier::new(3));
        let staged = Arc::new(Barrier::new(2));
        let consume = Arc::new(Barrier::new(2));
        let cleanup = Arc::new(Barrier::new(2));
        let observe = Arc::new(Barrier::new(3));
        let release = Arc::new(Barrier::new(3));
        let records = Arc::new(Mutex::new(([[(0usize, 0usize, 0usize); 2]; 2], [false; 2])));
        let handles: [_; 2] = std::array::from_fn(|index| {
            let a = allowance.clone();
            let start = start.clone();
            let staged = staged.clone();
            let consume = consume.clone();
            let records = records.clone();
            let child = reader.child.clone();
            let observe = observe.clone();
            let release = release.clone();
            let cleanup = cleanup.clone();
            let counter = reader.counter;
            std::thread::spawn(move || {
                start.wait();
                // Serialize allocation observation only: both genuine entries
                // remain alive together, both paid from the SAME captured admission.
                if index == 1 {
                    staged.wait();
                }
                let guard = reader_requests_for_test(counter, || a.try_enter_native()).ok();
                let requests = NATIVE_REQUESTS.with(std::cell::Cell::get);
                let count = NATIVE_REQUEST_COUNT.with(std::cell::Cell::get);
                let receipt = StorageAdmissionScope::receipt_layout_for_scan_test();
                let cost = receipt.size() + ScanAllowance::node_layout_for_test().size();
                let valid = count == 2
                    && guard.as_ref().is_some_and(|guard| {
                        let identity = guard.allocation_observation_for_test();
                        requests[1] == (identity.0, identity.1.size(), identity.1.align())
                            && requests[0] == (identity.2, receipt.size(), receipt.align())
                    })
                    && source_request_credit_for_test()[..2] == [cost, cost]
                    && source_unfunded_peak_for_test().0 == 0;
                {
                    let mut records = records.lock().unwrap();
                    records.0[index].copy_from_slice(&requests[..2]);
                    records.1[index] = valid;
                }
                if index == 0 {
                    staged.wait();
                }
                observe.wait();
                release.wait();
                consume.wait();
                // Both entries are live together; each is its own thread's
                // current node and draws on no fixed growth allowance.
                let current = ScanAllowance::current_node_for_test();
                let own_current = guard
                    .as_ref()
                    .is_some_and(|guard| guard.allocation_observation_for_test().0 == current);
                let no_growth = !ScanAllowance::take_current_growth_for_test(1);
                consume.wait();
                // Stage only physical cleanup, so another thread's legitimate
                // refund cannot obscure either side of the exact System.free cut.
                if index != first {
                    cleanup.wait();
                }
                drop(guard);
                if index == first {
                    cleanup.wait();
                }
                let tail = ScanAllowance::current_node_for_test();
                let native_left = redb::native_storage_active();
                drop(a);
                drop(child);
                (valid, own_current, no_growth, tail, native_left)
            })
        });
        start.wait();
        observe.wait();
        let (watched, valid) = *records.lock().unwrap();
        let all = [watched[0][0], watched[0][1], watched[1][0], watched[1][1]];
        if valid.into_iter().all(|valid| valid) {
            watch_source_free_for_test(&all, reader.counter);
        }
        let _watches = Watches;
        release.wait();
        // Finish BOTH workers before asserting: a deliberately broken native
        // producer must fail a proof without stranding its sibling at a barrier.
        let outcomes = handles.map(|handle| handle.join());
        for outcome in outcomes {
            assert_eq!(outcome.unwrap(), (true, true, true, 0, false));
        }
        assert_eq!(
            reader.used(),
            standing,
            "both threads return every node/receipt invoice"
        );
        let free = source_free_for_test(4);
        for index in 0..4 {
            let own_pair = all[index / 2 * 2].1 + all[index / 2 * 2 + 1].1;
            let live_pairs = if index / 2 == first { 2 } else { 1 };
            let paid = standing + live_pairs * own_pair;
            assert_eq!(
                free[index],
                (paid, paid, 1),
                "each thread's exact original node/receipt invoices survive both physical-free cuts"
            );
        }
        disarm_source_free_watch();
        assert_ne!(
            watched[0][1].0, watched[1][1].0,
            "simultaneously live thread-bound nodes differ"
        );
        // A continuation admits a fresh paid entry from the same captured
        // admission after the threads' entries are gone.
        let (clone, _, count, _) = capture(&reader, || allowance.clone());
        assert_eq!(count, 0);
        let ((), _, count) = native_requests_for_test(|| {
            let _guard = clone.try_enter_native().unwrap();
            assert!(!ScanAllowance::take_current_growth_for_test(1));
        });
        assert_eq!(count, 2);
        assert_eq!(reader.used(), standing);
        drop((allowance, clone));
        reader.finish();
    }
}

#[test]
#[serial]
fn legacy_scan_interfaces_remain_functional_without_native_funding_claims() {
    let allowance = ScanAllowance::default();
    let guard = allowance.enter();
    let helper = redb::scan_reads();
    drop(guard);
    redb::with_scan_reads(|| assert!(ScanAllowance::take_current_growth_for_test(16 * 1024)));
    assert!(!ScanAllowance::take_current_growth_for_test(1));
    drop(helper);
    assert_eq!(ScanAllowance::current_node_for_test(), 0);
    assert!(matches!(
        allowance.try_enter_native(),
        Err(redb::StorageError::Native(
            redb::NativeColdReadError::AdmissionRefused
        ))
    ));
}
