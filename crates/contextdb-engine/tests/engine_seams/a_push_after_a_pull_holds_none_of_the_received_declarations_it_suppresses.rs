//! A push never re-sends the declarations an edge received from its hub, and
//! it drops them as it walks: none reaches the final filter, the validators
//! hold nothing for them, and nightly the push costs the same memory however
//! many there are.
#![expect(
    clippy::result_large_err,
    reason = "The measured operations return the public Error, which keeps inline NativeRefusal diagnostics."
)]
use super::outbound_declaration_walk_support::*;

/// One post-pull push at `size`; returns (size, LSN at the push, peak).
fn post_pull_push(size: usize, nightly: bool) -> (usize, contextdb_core::Lsn, usize) {
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
    let since = network.client.push_watermark();
    let history = occurrences(&network.edge.db);
    let expected = expected_walk(&history, since, None);
    assert_eq!(
        expected.dropped.len(),
        size + 4 + usize::from(nightly),
        "the hub's churn, its three-statement unit and the echoed notes declaration (and, nightly, the echoed pad table) are all received above the frontier"
    );
    assert!(expected.collected.is_empty(), "{expected:?}");
    let hub_schema = occurrences(&network.hub.db);
    let pushed = measure(&network.edge.db, || runtime.block_on(network.client.push()));
    receipt("post_pull_push", size, &pushed);
    let applied = pushed.value.as_ref().expect("the post-pull push succeeds");
    assert_eq!(applied.applied_rows, 1, "the one new row reaches the hub");
    assert_eq!(
        occurrences(&network.hub.db),
        hub_schema,
        "a push after a pull leaves the hub's schema unchanged"
    );
    assert_eq!(ids(&network.hub.db, "notes"), vec![1, 2]);
    if !nightly {
        assert_one_epoch("post-pull push", &pushed.work, None, true);
    }
    assert_sent("post-pull push", &pushed.work, &[]);
    if !nightly {
        assert_walk("post-pull push", &pushed.work, &expected);
    }
    let sample = (size, pushed.lsn, pushed.peak);
    network.close(&runtime);
    sample
}

#[test]
#[ignore = "A push after a pull still holds the received declarations it suppresses."]
fn a_push_after_a_pull_holds_none_of_the_received_declarations_it_suppresses() {
    post_pull_push(PR_SIZE, false);
}

#[cfg(feature = "nightly-tests")]
#[test]
#[ignore = "a push after a pull receives a different number of declarations above the frontier than the hub sent"]
fn a_push_after_a_pull_holds_none_of_the_received_declarations_it_suppresses_as_received_history_grows()
 {
    let measured = NIGHTLY_SIZES.map(|size| post_pull_push(size, true));
    let control = NIGHTLY_SIZES.map(|size| control_sample(size, || post_pull_push(PR_SIZE, true)));
    assert_bounded_by_control("post-pull push", &measured, &control, BallastIn::NoWalk);
}
