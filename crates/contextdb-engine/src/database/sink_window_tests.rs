//! The bounded sink window's protocol, without a dispatcher: appends, refill
//! planning, install refusal, removal bookkeeping and the decoded charge the
//! window's owner holds.
use super::*;
use std::sync::Arc;

fn entry(id: u64) -> SinkQueueEntry {
    SinkQueueEntry {
        id,
        event: SinkEvent {
            action_id: id,
            event_type: "ev_insert".into(),
            table: "ev".into(),
            row_values: HashMap::from([("id".to_string(), Value::Int64(id as i64))]),
            severity: String::new(),
            at_lsn: Lsn(id),
        },
        row_id: RowId(id),
        attempts: 0,
        gate: EventGateSnapshot::default(),
    }
}

fn owner() -> Arc<crate::read_image_memory::ImageMemory> {
    crate::read_image_memory::ImageMemory::new(None)
}

/// A window of these ids, each charged `credit` on a fresh owner.
fn window(ids: &[u64], credit: usize) -> SinkWindow {
    let owner = owner();
    owner.reserve(ids.len() * credit).unwrap();
    SinkWindow {
        entries: ids.iter().map(|id| entry(*id)).collect(),
        credits: ids.iter().map(|_| credit).collect(),
        owner: Some(owner),
    }
}

fn bounded() -> SinkQueue {
    SinkQueue {
        bounded: true,
        ..SinkQueue::default()
    }
}

fn ids(queue: &SinkQueue) -> Vec<u64> {
    queue.entries.iter().map(|entry| entry.id).collect()
}

#[test]
fn a_bounded_append_is_counted_and_left_for_the_next_window_read() {
    let mut queue = bounded();
    queue.publish_append(entry(1));
    queue.publish_append(entry(2));
    // Only a window read decodes and funds a resident entry, so an append is
    // never resident ahead of an entry an earlier read passed over (C1).
    assert_eq!(
        (ids(&queue), queue.depth(), queue.all_resident()),
        (vec![], 2, false)
    );
    let mut unbounded = SinkQueue::default();
    unbounded.publish_append(entry(1));
    assert_eq!((ids(&unbounded), unbounded.depth()), (vec![1], 1));
}

#[test]
fn a_refill_continues_its_sweep_then_wraps_to_the_lowest_id() {
    let mut queue = bounded();
    queue.resident_through = 10;
    queue.sweep_upper = 40;
    let plan = queue.plan_refill(7);
    assert_eq!(
        (plan.after, plan.upper, queue.pending_refill),
        (Some(10), Some(40), Some(7))
    );
    queue.resident_through = 40;
    let plan = queue.plan_refill(8);
    assert_eq!(
        (plan.after, plan.upper),
        (None, None),
        "an exhausted sweep wraps"
    );
}

#[test]
fn a_removal_published_after_the_plan_refuses_its_install_forward_or_wrapping() {
    for (resident_through, sweep_upper) in [(10, 40), (40, 40)] {
        let mut queue = bounded();
        queue.depth = 30;
        queue.resident_through = resident_through;
        queue.sweep_upper = sweep_upper;
        let plan = queue.plan_refill(1);
        // An acknowledgement removes id 1 (below the forward range, inside a wrap's).
        queue.publish_removals(&HashSet::from([1]), 1);
        let read = window(&[1, 2], 100);
        let read_owner = read.owner.clone().unwrap();
        assert!(
            !queue.install_refill(&plan, read, 41, false),
            "a stale window never re-admits a removed event"
        );
        assert!(queue.entries.is_empty());
        assert_eq!(
            (queue.depth(), queue.resident_through, queue.sweep_upper),
            (29, resident_through, sweep_upper)
        );
        assert_eq!(queue.pending_refill, None);
        assert_eq!(
            Arc::strong_count(&read_owner),
            1,
            "the refused window is destroyed with its charge"
        );
    }
    // Another plan replaced this one: refused too.
    let mut queue = bounded();
    let stale = queue.plan_refill(1);
    let _current = queue.plan_refill(2);
    assert!(!queue.install_refill(&stale, window(&[5], 10), 5, true));
}

#[test]
fn a_purge_of_an_entry_outside_the_window_refuses_a_wrapping_refill() {
    // Sol's counterexample on the purge path: a wrap is planned, a purge
    // removes an entry that is not resident (so no resident removal bumps
    // the generation), and the wrap's read still holds it.
    let bus = EventBusState::new();
    bus.queues.lock().insert(
        "s".into(),
        SinkQueue {
            bounded: true,
            depth: 30,
            resident_through: 40,
            sweep_upper: 40,
            ..SinkQueue::default()
        },
    );
    let plan = bus.queues.lock().get_mut("s").unwrap().plan_refill(1);
    assert_eq!((plan.after, plan.upper), (None, None));
    let drain = bus.begin_authoritative_purge_queue_drain().unwrap();
    let token = drain.freeze_for_commit_locked_purge();
    let mut publication = bus
        .prepare_authoritative_purge_queue_replacement(
            token,
            BTreeSet::from(["s".into()]),
            &BTreeMap::new(),
        )
        .unwrap();
    publication.record_durable_sink_removal("s");
    bus.publish_prepared_authoritative_purge_queue_replacement(publication);
    let mut queues = bus.queues.lock();
    let queue = queues.get_mut("s").unwrap();
    assert_eq!(queue.depth(), 29);
    assert!(
        !queue.install_refill(&plan, window(&[1, 2], 100), 41, false),
        "the purged entry stays purged"
    );
    assert!(queue.entries.is_empty());
}

#[test]
fn removals_return_each_entrys_own_decoded_credit() {
    let mut queue = bounded();
    queue.depth = 10;
    let plan = queue.plan_refill(1);
    let read = window(&[1, 2, 3], 100);
    let held = read.owner.clone().unwrap();
    assert!(queue.install_refill(&plan, read, 10, false));
    assert_eq!((queue.resident_bytes(), held.held()), (300, 300));
    queue.publish_removals(&HashSet::from([2]), 3);
    assert_eq!((ids(&queue), queue.depth()), (vec![1, 3], 7));
    assert_eq!((queue.resident_bytes(), held.held()), (200, 200));
}

#[test]
fn a_copy_charge_holds_until_its_copies_are_gone() {
    let owner = owner();
    let charge = SinkWindowCopyCharge::reserve(&owner, 500).unwrap();
    assert_eq!(owner.held(), 500);
    drop(charge);
    assert_eq!(owner.held(), 0);
    let tight = crate::read_image_memory::ImageMemory::new(Some(10));
    assert!(matches!(
        SinkWindowCopyCharge::reserve(&tight, 11),
        Err(Error::MemoryBudgetExceeded { .. })
    ));
    assert_eq!(tight.held(), 0);
}

/// An event bus holding one bounded window of `ids` on `owner`.
fn bus_with_window(
    owner: &Arc<crate::read_image_memory::ImageMemory>,
    ids: &[u64],
    credit: usize,
) -> EventBusState {
    owner.reserve(ids.len() * credit).unwrap();
    let bus = EventBusState::new();
    bus.queues.lock().insert(
        "s".into(),
        SinkQueue {
            entries: ids.iter().map(|id| entry(*id)).collect(),
            credits: ids.iter().map(|_| credit).collect(),
            owner: Some(owner.clone()),
            bounded: true,
            depth: ids.len() as u64,
            resident_through: *ids.last().unwrap(),
            sweep_upper: *ids.last().unwrap(),
            ..SinkQueue::default()
        },
    );
    bus
}

fn prepare(
    bus: &EventBusState,
    rows: &[u64],
) -> Result<PreparedAuthoritativePurgeEventBusPublication> {
    let drain = bus.begin_authoritative_purge_queue_drain().unwrap();
    let token = drain.freeze_for_commit_locked_purge();
    bus.prepare_authoritative_purge_queue_replacement(
        token,
        BTreeSet::from(["s".into()]),
        &BTreeMap::from([("ev".to_string(), rows.iter().map(|id| RowId(*id)).collect())]),
    )
}

#[test]
fn a_dropped_purge_preparation_leaves_the_window_charged_exactly_as_before() {
    let owner = owner();
    let bus = bus_with_window(&owner, &[1, 2, 3], 100);
    let before = owner.held();
    for _ in 0..2 {
        let publication = prepare(&bus, &[2]).unwrap();
        assert!(
            owner.held() > before,
            "the replacement copy is funded while it exists"
        );
        drop(publication);
        assert_eq!(
            owner.held(),
            before,
            "an aborted purge returns exactly what it took"
        );
        let queues = bus.queues.lock();
        assert_eq!((ids(&queues["s"]), queues["s"].depth()), (vec![1, 2, 3], 3));
    }
    // The original entries still own their credit: removing them returns it
    // once, to zero.
    bus.queues
        .lock()
        .get_mut("s")
        .unwrap()
        .publish_removals(&HashSet::from([1, 2, 3]), 3);
    assert_eq!(owner.held(), 0);
}

#[test]
fn a_published_purge_returns_the_purged_entrys_credit_once() {
    let owner = owner();
    let bus = bus_with_window(&owner, &[1, 2, 3], 100);
    let publication = prepare(&bus, &[2]).unwrap();
    bus.publish_prepared_authoritative_purge_queue_replacement(publication);
    assert_eq!(ids(&bus.queues.lock()["s"]), vec![1, 3]);
    assert_eq!(owner.held(), 200);
}

#[test]
fn a_purge_replacement_funds_the_copied_window_buffers_before_copying() {
    // Room for the copied entries' decoded bytes but not their buffers.
    let tight = crate::read_image_memory::ImageMemory::new(Some(2 * 300 + 8));
    let bus = bus_with_window(&tight, &[1, 2, 3], 100);
    let before = tight.held();
    let refused = prepare(&bus, &[2]).map(|_| ()).unwrap_err();
    assert!(
        matches!(refused, Error::MemoryBudgetExceeded { .. }),
        "{refused:?}"
    );
    assert_eq!(tight.held(), before, "a refused copy takes nothing");
    assert_eq!(ids(&bus.queues.lock()["s"]), vec![1, 2, 3]);
}

#[test]
#[serial_test::serial]
fn an_attempt_rewrite_copies_no_part_of_the_entry() {
    use crate::persistence::content_identity::proof_allocation_oracle_tests::measured;
    let mut wide = entry(9);
    wide.event
        .row_values
        .insert("body".into(), Value::Text("x".repeat(256 * 1024)));
    let (result, peak, retained) = measured(|| {
        with_rewritten_attempts(&mut wide, 3 | UNCONFIRMED_ATTEMPT, |updated| {
            assert_eq!(updated.attempts, 3 | UNCONFIRMED_ATTEMPT);
            Ok(())
        })
    });
    result.unwrap();
    assert_eq!(
        (peak, retained),
        (0, 0),
        "the rewrite borrows the resident entry"
    );
    assert_eq!(wide.attempts, 3 | UNCONFIRMED_ATTEMPT);
    let refused = with_rewritten_attempts(&mut wide, 4, |_| -> Result<()> {
        Err(Error::Other("durable write refused".into()))
    });
    assert!(refused.is_err());
    assert_eq!(
        wide.attempts,
        3 | UNCONFIRMED_ATTEMPT,
        "a refused rewrite leaves the entry as it was"
    );
}
