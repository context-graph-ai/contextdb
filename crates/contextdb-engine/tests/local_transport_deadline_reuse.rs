#![cfg(feature = "test-seams")]
//! Bounded outcomes on the exported deadline surface of the local owner
//! channel.
//!
//! Every entry point in this surface is public, so a caller can reach it with
//! any ordering of calls. The contract is that misuse produces a typed,
//! bounded result: a completed operation stays inert when it is driven again,
//! and a clock told to move backwards refuses the update while remaining
//! usable. Neither may take the owner process down, and neither may leave the
//! clock unable to answer later callers.

use contextdb_core::read_contract::DeadlineClock;
use contextdb_engine::local_transport::{
    LocalDeadlineOperation, ManualDeadlineClock, serve_request_with_deadline,
};
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

#[test]
fn driving_a_completed_deadline_operation_again_yields_a_typed_result() {
    let clock = ManualDeadlineClock::at(0);
    let operation: LocalDeadlineOperation<'_> = Box::pin(std::future::ready(Ok(())));
    let mut wait = serve_request_with_deadline(&clock, 1_000, operation);
    let mut context = Context::from_waker(Waker::noop());

    assert!(
        matches!(Pin::new(&mut wait).poll(&mut context), Poll::Ready(Ok(()))),
        "the operation completes on its first drive"
    );

    let driven_again = catch_unwind(AssertUnwindSafe(|| Pin::new(&mut wait).poll(&mut context)));
    let Ok(outcome) = driven_again else {
        panic!(
            "driving a completed deadline operation again takes the owner process down instead \
             of returning a typed result"
        );
    };
    assert!(
        matches!(outcome, Poll::Pending | Poll::Ready(Err(_))),
        "a completed deadline operation must stay inert when driven again, observed {outcome:?}"
    );
}

#[test]
fn a_backwards_clock_update_is_refused_and_leaves_the_clock_usable() {
    let clock = ManualDeadlineClock::at(1_000);

    let update = catch_unwind(AssertUnwindSafe(|| {
        clock.advance_to(500);
    }));
    assert!(
        update.is_ok(),
        "a backwards clock update takes the caller down instead of being refused"
    );

    assert_eq!(
        clock.now_ms(),
        1_000,
        "a refused backwards update must leave monotonic time intact"
    );
}

#[test]
fn a_backwards_clock_update_from_another_caller_leaves_the_clock_answering() {
    let clock = ManualDeadlineClock::at(1_000);
    let misusing = clock.clone();
    // The misusing caller may or may not survive; what the clock owes every
    // OTHER caller is that it keeps working afterwards.
    let _ = std::thread::spawn(move || {
        misusing.advance_to(500);
    })
    .join();

    assert_eq!(
        clock.now_ms(),
        1_000,
        "a clock that has seen a backwards update must keep answering every later caller"
    );
    assert_eq!(
        clock.registered_waiter_count(),
        0,
        "a clock that has seen a backwards update must keep reporting its waiters"
    );
}

#[test]
fn a_forward_clock_update_still_moves_time() {
    let clock = ManualDeadlineClock::at(1_000);
    clock.advance_to(1_500);
    assert_eq!(clock.now_ms(), 1_500);
}
