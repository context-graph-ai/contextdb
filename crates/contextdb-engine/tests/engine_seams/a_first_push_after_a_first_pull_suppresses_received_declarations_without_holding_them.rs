//! An edge whose first sync is a pull pushes from frontier 0 next. That first
//! push sends its own new row and none of the hub's declarations it received,
//! dropping them as it walks. The hub's history arrives in a fixed number of
//! commits, so the edge's LSN at the push is the same at every size; nightly,
//! the push costs the same memory however long that history is.
#![expect(
    clippy::result_large_err,
    reason = "The measured operations return the public Error, which keeps inline NativeRefusal diagnostics."
)]
use super::outbound_declaration_walk_support::*;
use contextdb_core::Lsn;

fn first_push_after_first_pull(size: usize, nightly: bool) -> (usize, Lsn, usize) {
    let _clock = fixed_clock();
    let runtime = runtime();
    let directory = tempfile::tempdir().unwrap();
    let edge = Peer::new(directory.path(), "edge", 0x11);
    let hub = Peer::new(directory.path(), "hub", 0x22);
    sql(
        &hub.db,
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) SYNC TWO WAY",
    );
    grouped_churn(&hub.db, "hub_history", size);
    sql(&hub.db, "INSERT INTO notes VALUES (1, 'first')");
    let network = Network::connect(&runtime, edge, hub);
    runtime.block_on(network.client.pull_default()).unwrap();
    store_ballast(&network.edge.db, "hub_history_0_table");
    assert_eq!(ids(&network.edge.db, "notes"), vec![1]);
    assert_eq!(
        network.client.push_watermark(),
        Lsn(0),
        "the first push starts from frontier 0"
    );
    sql(&network.edge.db, "INSERT INTO notes VALUES (2, 'second')");
    let expected = expected_walk(&occurrences(&network.edge.db), Lsn(0), None);
    assert_eq!(
        expected.dropped.len(),
        size + 1,
        "the hub's history and its notes declaration are all received: {expected:?}"
    );
    assert!(expected.collected.is_empty(), "{expected:?}");
    let hub_schema = occurrences(&network.hub.db);
    let pushed = measure(&network.edge.db, || runtime.block_on(network.client.push()));
    receipt("first_push_after_first_pull", size, &pushed);
    assert_eq!(
        pushed
            .value
            .as_ref()
            .expect("the first push succeeds")
            .applied_rows,
        1
    );
    assert_eq!(occurrences(&network.hub.db), hub_schema);
    assert_eq!(ids(&network.hub.db, "notes"), vec![1, 2]);
    if !nightly {
        assert_one_epoch("first push after a first pull", &pushed.work, None, true);
    }
    assert_sent("first push after a first pull", &pushed.work, &[]);
    if !nightly {
        assert_walk("first push after a first pull", &pushed.work, &expected);
    }
    let sample = (size, pushed.lsn, pushed.peak);
    network.close(&runtime);
    sample
}

#[test]
fn a_first_push_after_a_first_pull_suppresses_received_declarations_without_holding_them() {
    first_push_after_first_pull(PR_SIZE, false);
}

#[cfg(feature = "nightly-tests")]
#[test]
fn a_first_push_after_a_first_pull_suppresses_received_declarations_without_holding_them_as_received_history_grows()
 {
    let measured = NIGHTLY_SIZES.map(|size| first_push_after_first_pull(size, true));
    let control = NIGHTLY_SIZES
        .map(|size| control_sample(size, || first_push_after_first_pull(PR_SIZE, true)));
    assert_bounded_by_control(
        "first push after a first pull",
        &measured,
        &control,
        BallastIn::TheWalk,
    );
}
