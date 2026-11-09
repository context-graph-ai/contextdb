//! Counting pending work right after a pull, before any push, reports no
//! received declaration, leaves the push frontier where it was, reads its one
//! epoch value before walking history, and drops every received declaration
//! as it walks. The push that follows from that same frontier does the same.
//! Nightly, both cost the same memory however many declarations were
//! received.
#![expect(
    clippy::result_large_err,
    reason = "The measured operations return the public Error, which keeps inline NativeRefusal diagnostics."
)]
use super::outbound_declaration_walk_support::*;
use contextdb_core::Lsn;

/// Returns (size, LSN, pending peak) and (size, LSN, push peak).
fn pending_then_push(size: usize, nightly: bool) -> [(usize, Lsn, usize); 2] {
    let _clock = fixed_clock();
    let runtime = runtime();
    let directory = tempfile::tempdir().unwrap();
    let network = pulled_edge(
        &runtime,
        directory.path(),
        size,
        nightly_pad(size, nightly),
        &|_| {},
    );
    sql(&network.edge.db, "INSERT INTO notes VALUES (2, 'second')");
    let frontier = network.client.push_watermark();
    let expected = expected_walk(&occurrences(&network.edge.db), frontier, None);
    assert_eq!(
        expected.dropped.len(),
        size + 4 + usize::from(nightly),
        "{expected:?}"
    );
    let pending = measure(&network.edge.db, || {
        network.client.pending_push_change_count()
    });
    receipt("pending_after_pull", size, &pending);
    assert_eq!(
        *pending.value.as_ref().expect("the pending read succeeds"),
        1,
        "the one new row is pending and no received declaration is"
    );
    assert_eq!(
        network.client.push_watermark(),
        frontier,
        "inspecting pending work leaves the push frontier unchanged"
    );
    let hub_schema = occurrences(&network.hub.db);
    let pushed = measure(&network.edge.db, || runtime.block_on(network.client.push()));
    receipt("push_after_pending", size, &pushed);
    assert_eq!(
        pushed
            .value
            .as_ref()
            .expect("the push succeeds")
            .applied_rows,
        1
    );
    assert_eq!(occurrences(&network.hub.db), hub_schema);
    assert_eq!(ids(&network.hub.db, "notes"), vec![1, 2]);
    if !nightly {
        assert_one_epoch("push from the inspected frontier", &pushed.work, None, true);
    }
    assert_sent("push from the inspected frontier", &pushed.work, &[]);
    if !nightly {
        assert_walk("push from the inspected frontier", &pushed.work, &expected);
    }
    assert_sent("pending read after a pull", &pending.work, &[]);
    if !nightly {
        assert_one_epoch("pending read after a pull", &pending.work, None, true);
    }
    if !nightly {
        assert_walk("pending read after a pull", &pending.work, &expected);
    }
    let samples = [
        (size, pending.lsn, pending.peak),
        (size, pushed.lsn, pushed.peak),
    ];
    network.close(&runtime);
    samples
}

#[test]
#[ignore = "A pending read after a pull still walks the received declarations it suppresses."]
fn a_pending_read_after_a_pull_holds_none_of_the_received_declarations_it_suppresses() {
    pending_then_push(PR_SIZE, false);
}

#[cfg(feature = "nightly-tests")]
#[test]
#[ignore = "a pending read after a pull holds received declarations that it should suppress as received history grows"]
fn a_pending_read_after_a_pull_holds_none_of_the_received_declarations_it_suppresses_as_received_history_grows()
 {
    let [small, large] = NIGHTLY_SIZES.map(|size| pending_then_push(size, true));
    let [small_control, large_control] =
        NIGHTLY_SIZES.map(|size| control_sample(size, || pending_then_push(PR_SIZE, true)));
    assert_bounded_by_control(
        "pending read after a pull",
        &[small[0], large[0]],
        &[small_control[0], large_control[0]],
        BallastIn::NoWalk,
    );
    assert_bounded_by_control(
        "push from the inspected frontier",
        &[small[1], large[1]],
        &[small_control[1], large_control[1]],
        BallastIn::NoWalk,
    );
}
