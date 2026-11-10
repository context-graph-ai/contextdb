use super::{ALLOWANCE, Budget};
use crate::types::Error;

/// A budget with `room` bytes left after its allowance, and what it counts as used when new.
fn budget_with_room(room: u64) -> (Budget, u64) {
    let limit = ALLOWANCE + room;
    let budget = Budget::new(Some(limit)).expect("a limit that holds the allowance");
    assert_eq!(budget.limit(), Some(limit));
    let baseline = budget.used();
    (budget, baseline)
}

#[test]
fn bytes_are_counted_while_held_and_given_back_when_released() {
    let (budget, baseline) = budget_with_room(100);
    let mut held = budget.reserve(60, "sort").expect("60 of 100");
    held.grow(30).expect("30 of the 40 left");
    assert_eq!((held.bytes(), budget.used()), (90, baseline + 90));
    held.shrink(50);
    assert_eq!((held.bytes(), budget.used()), (40, baseline + 40));
    // Another handle is the same budget.
    let other = budget.clone().reserve(60, "result").expect("60 of 60");
    assert_eq!(budget.used(), baseline + 100);
    drop(held);
    drop(other);
    assert_eq!(budget.used(), baseline);
}

#[test]
fn a_refusal_names_what_was_asked_and_what_was_left_and_counts_nothing() {
    let (budget, baseline) = budget_with_room(100);
    let mut held = budget.reserve(60, "sort").expect("60 of 100");
    for refusal in [budget.reserve(41, "sort").err(), held.grow(41).err()] {
        match refusal {
            Some(Error::MemoryBudgetExceeded {
                operation: "sort",
                requested_bytes: 41,
                available_bytes: 40,
                budget_limit_bytes,
                ..
            }) => assert_eq!(budget_limit_bytes, ALLOWANCE + 100),
            other => panic!("41 bytes of 40 gave {other:?}"),
        }
    }
    assert_eq!((held.bytes(), budget.used()), (60, baseline + 60));
    held.grow(40).expect("exactly what was left");
}

#[test]
fn with_no_limit_every_reservation_succeeds_and_is_counted() {
    let budget = Budget::new(None).expect("no ceiling");
    assert_eq!(budget.limit(), None);
    let baseline = budget.used();
    let held = budget.reserve(1 << 50, "result").expect("no ceiling");
    assert_eq!(budget.used(), baseline + (1 << 50));
    drop(held);
    assert_eq!(budget.used(), baseline);
}

#[test]
fn a_limit_below_what_is_in_use_is_refused_and_the_old_one_stays() {
    assert!(Budget::new(Some(ALLOWANCE - 1)).is_err());
    let (budget, baseline) = budget_with_room(100);
    let held = budget.reserve(60, "result").expect("60 of 100");
    match budget.set_limit(Some(baseline + 59)) {
        Err(Error::MemoryBudgetExceeded {
            requested_bytes,
            available_bytes,
            ..
        }) => assert_eq!(
            (requested_bytes, available_bytes),
            (baseline + 60, baseline + 59)
        ),
        other => panic!("a limit one byte below what is in use gave {other:?}"),
    }
    assert_eq!(budget.limit(), Some(ALLOWANCE + 100));
    budget
        .set_limit(Some(baseline + 60))
        .expect("exactly what is in use");
    assert!(budget.reserve(1, "result").is_err());
    budget.set_limit(None).expect("no ceiling");
    assert_eq!(budget.limit(), None);
    drop(held);
    assert_eq!(budget.used(), baseline);
}
