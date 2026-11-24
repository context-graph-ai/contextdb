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

// M4: the default limit is a quarter of what the process may use, never below the door's floor,
// and 2 GiB where that is unknown; a process allowed 32 MiB gets the floor, not 8 MiB.
#[test]
fn a_process_allowed_32_mib_gets_the_floor_as_its_default() {
    let floor = (10 << 20) + (768 << 10);
    assert_eq!(super::default_limit(Some(32 << 20), floor), floor);
    assert_eq!(super::default_limit(None, floor), 2 << 30);
    assert_eq!(super::default_limit(Some(64 << 30), floor), 16 << 30);
}
