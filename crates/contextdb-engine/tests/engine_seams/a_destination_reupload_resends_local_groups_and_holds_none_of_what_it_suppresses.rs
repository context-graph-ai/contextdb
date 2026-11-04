//! After an edge moves to a new hub, its re-upload resends every local
//! declaration group it held at the move, whole and in order, each naming the
//! edge as author with its source LSN and ordinal, and drops every
//! declaration it received after the move as it walks. One epoch value, read
//! before the walk, governs the final filter, the delete synthesis and the
//! arrival filter, on the pending read and on the push. A data row that
//! arrived by sync from the new hub above the move's frontier is not resent,
//! and the pending read counts exactly the local groups and the edge's own
//! row. Nightly, the accountant peak grows by no more than the store-size
//! term.
//!
//! Two boundaries are left out on open defects: a row received at or below
//! the frontier needs a pull before the move, and that re-upload refuses on
//! CDB-235; a row deleted at or below the frontier is resurrected on the new
//! hub by CDB-237. The nightly control's ballast sits inside the re-upload's
//! walk from 0, so that row stays RED on CDB-243 until a re-upload stops
//! holding the never-synced rows it walks past.
#![expect(
    clippy::result_large_err,
    reason = "The measured operations return the public Error, which keeps inline NativeRefusal diagnostics."
)]
use super::outbound_declaration_walk_support::*;
use contextdb_core::Lsn;

fn reupload_local(size: usize, nightly: bool) -> [(usize, Lsn, usize); 2] {
    let _clock = fixed_clock();
    let runtime = runtime();
    let directory = tempfile::tempdir().unwrap();
    let moved = moved_edge(
        &runtime,
        directory.path(),
        size,
        Prefix::AuthoredByTheEdge,
        true,
        &|_| {},
    );
    let network = &moved.network;
    let edge_schema = occurrences(&network.edge.db);
    let held_at_move = edge_schema
        .iter()
        .filter(|occurrence| occurrence.lsn <= moved.frontier)
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        held_at_move.iter().all(|occurrence| !occurrence.received),
        "everything held at the move is local"
    );
    assert_eq!(
        held_at_move
            .iter()
            .filter(|occurrence| names(&occurrence.ddl, "prefix_a"))
            .count(),
        2
    );
    let since = network.client.push_watermark();
    let pending_expected = expected_walk(&edge_schema, since, Some(moved.frontier));
    let expected = expected_walk(&edge_schema, REUPLOAD_PUSH_SINCE, Some(moved.frontier));
    assert_eq!(
        expected.dropped.len(),
        size + 1,
        "exactly the second hub's declarations, hub_rows among them, arrive above F: {expected:?}"
    );
    assert_eq!(
        ids(&network.edge.db, "hub_rows"),
        vec![20],
        "row 20 arrived by sync from the new hub, above F"
    );
    assert_eq!(
        expected.collected,
        held_at_move
            .iter()
            .map(|occurrence| (occurrence.lsn, occurrence.ordinal))
            .collect::<Vec<_>>(),
        "every local group held at the move reaches the final filter"
    );

    let pending = measure(&network.edge.db, || {
        network.client.pending_push_change_count()
    });
    receipt("destination_reupload_local_pending", size, &pending);
    assert_eq!(
        *pending
            .value
            .as_ref()
            .expect("the pending read under an armed epoch succeeds"),
        held_at_move.len() + 1,
        "pending is exactly the local declarations held at the move plus the edge's one row; nothing received above F, declaration or row 20, is pending"
    );
    assert_eq!(
        network.client.push_watermark(),
        since,
        "inspecting pending work leaves the push frontier unchanged"
    );
    let hub_before = occurrences(&network.hub.db);
    let pushed = measure(&network.edge.db, || runtime.block_on(network.client.push()));
    receipt("destination_reupload_local", size, &pushed);
    assert_eq!(
        pushed
            .value
            .as_ref()
            .expect("the re-upload succeeds")
            .applied_rows,
        1,
        "the re-upload applies the edge's one row and does not resend row 20, which arrived from this hub above F"
    );
    assert_eq!(
        ids(&network.hub.db, "notes"),
        vec![1],
        "the new hub gets the edge's one row"
    );
    assert_eq!(ids(&network.hub.db, "hub_rows"), vec![20]);
    let resent = occurrences(&network.hub.db)[hub_before.len()..].to_vec();
    let edge_node = network.edge.node_id();
    let expected_resent = held_at_move.clone();
    assert_eq!(
        resent
            .iter()
            .map(|occurrence| &occurrence.ddl)
            .collect::<Vec<_>>(),
        expected_resent
            .iter()
            .map(|occurrence| &occurrence.ddl)
            .collect::<Vec<_>>(),
        "exactly the local groups held at the move are resent, in order"
    );
    assert_eq!(
        resent
            .iter()
            .map(|occurrence| occurrence.author.clone())
            .collect::<Vec<_>>(),
        expected_resent
            .iter()
            .map(|occurrence| Some((edge_node.clone(), occurrence.lsn, occurrence.ordinal)))
            .collect::<Vec<_>>(),
        "each resent occurrence names the edge as author with its source LSN and ordinal"
    );
    if !nightly {
        assert_one_epoch(
            "destination re-upload of local groups",
            &pushed.work,
            Some(moved.frontier),
            true,
        );
    }
    assert_sent(
        "destination re-upload of local groups",
        &pushed.work,
        &sent_as_local(&edge_node, &expected_resent),
    );
    if !nightly {
        assert_one_epoch(
            "pending read under an armed epoch",
            &pending.work,
            Some(moved.frontier),
            true,
        );
    }
    if !nightly {
        assert_walk(
            "pending read under an armed epoch",
            &pending.work,
            &pending_expected,
        );
    }
    if !nightly {
        assert_walk(
            "destination re-upload of local groups",
            &pushed.work,
            &expected,
        );
    }
    let samples = [
        (size, pending.lsn, pending.peak),
        (size, pushed.lsn, pushed.peak),
    ];
    moved.network.close(&runtime);
    samples
}

#[test]
fn a_destination_reupload_resends_local_groups_and_holds_none_of_what_it_suppresses() {
    reupload_local(PR_SIZE, false);
}

#[cfg(feature = "nightly-tests")]
#[test]
fn a_destination_reupload_resends_local_groups_and_holds_none_of_what_it_suppresses_as_received_history_grows()
 {
    let [small, large] = NIGHTLY_SIZES.map(|size| reupload_local(size, true));
    let [small_control, large_control] =
        NIGHTLY_SIZES.map(|size| control_sample(size, || reupload_local(PR_SIZE, true)));
    assert_bounded_by_control(
        "pending read under an armed epoch",
        &[small[0], large[0]],
        &[small_control[0], large_control[0]],
        BallastIn::TheWalk,
    );
    assert_bounded_by_control(
        "destination re-upload of local groups",
        &[small[1], large[1]],
        &[small_control[1], large_control[1]],
        BallastIn::TheWalk,
    );
}
