//! After an edge moves to a new hub, its re-upload resends every declaration
//! it held at the move, local and received alike, each with its original
//! author, and drops what it received after the move as it walks. A received
//! group exactly at the move's frontier is held, so it is resent. Nightly, the
//! push costs the same memory at both sizes.
//!
//! Known separate defect (CDB-235): today an edge that ever pulled schema
//! cannot re-upload after a move at all. The push refuses with "table notes
//! has schema metadata but no authoritative generation", wrongly reported as
//! a corrupt store. So the full row is ignored until that is fixed, and the
//! companion row below pins that exact refusal together with the walk facts
//! that are already reachable: the refusal fires after extraction, so the
//! companion proves the received group at the frontier reaches the final
//! filter and everything received above it is dropped. When CDB-235 is fixed
//! the companion fails, which forces this row to be un-ignored.
#![expect(
    clippy::result_large_err,
    reason = "The measured operations return the public Error, which keeps inline NativeRefusal diagnostics."
)]
use super::outbound_declaration_walk_support::*;
use contextdb_core::{Error, Lsn};
use contextdb_engine::outbound_ddl_test_seams::SentDdl;

const CDB_235_REFUSAL: &str = "has schema metadata but no authoritative generation";

struct Setup {
    moved: MovedEdge,
    held_at_move: Vec<contextdb_engine::database::DdlOccurrenceForTest>,
    expected: Expected,
    since: Lsn,
}

fn received_prefix(
    runtime: &tokio::runtime::Runtime,
    directory: &std::path::Path,
    size: usize,
) -> Setup {
    let moved = moved_edge(
        runtime,
        directory,
        size,
        Prefix::ReceivedFromTheFirstHub,
        false,
        &|_| {},
    );
    let edge_schema = occurrences(&moved.network.edge.db);
    let held_at_move = edge_schema
        .iter()
        .filter(|occurrence| occurrence.lsn <= moved.frontier)
        .cloned()
        .collect::<Vec<_>>();
    let at_frontier = held_at_move
        .iter()
        .filter(|occurrence| occurrence.lsn == moved.frontier)
        .collect::<Vec<_>>();
    assert!(
        !at_frontier.is_empty() && at_frontier.iter().all(|occurrence| occurrence.received),
        "the pull just before the move leaves a whole received group exactly at F: {held_at_move:?}"
    );
    let since = REUPLOAD_PUSH_SINCE;
    let expected = expected_walk(&edge_schema, since, Some(moved.frontier));
    assert_eq!(
        expected.dropped.len(),
        size,
        "exactly the second hub's declarations arrive above F: {expected:?}"
    );
    assert!(
        at_frontier.iter().all(|occurrence| expected
            .collected
            .contains(&(occurrence.lsn, occurrence.ordinal))),
        "the received group at F is held, so it must reach the final filter: {expected:?}"
    );
    Setup {
        moved,
        held_at_move,
        expected,
        since,
    }
}

fn reupload_received_prefix(size: usize, nightly: bool) -> PushSample {
    let _clock = fixed_clock();
    let runtime = runtime();
    let directory = tempfile::tempdir().unwrap();
    let Setup {
        moved,
        held_at_move,
        expected,
        since,
    } = received_prefix(&runtime, directory.path(), size);
    let network = &moved.network;
    let edge_node = network.edge.node_id();
    let expected_resent = held_at_move
        .iter()
        .filter(|occurrence| occurrence.lsn > since)
        .map(|occurrence| SentDdl {
            source: (occurrence.lsn, occurrence.ordinal),
            author: if occurrence.received {
                occurrence.author.clone()
            } else {
                Some((edge_node.clone(), occurrence.lsn, occurrence.ordinal))
            },
        })
        .collect::<Vec<_>>();
    assert!(
        expected_resent.iter().any(|sent| sent
            .author
            .as_ref()
            .is_some_and(|(node, _, _)| *node == moved.first_hub_node)),
        "the held prefix includes declarations the first hub authored"
    );
    let hub_before = occurrences(&network.hub.db);
    let pushed = measure(&network.edge.db, || runtime.block_on(network.client.push()));
    receipt("destination_reupload", size, &pushed);
    pushed
        .value
        .as_ref()
        .expect("an edge holding received schema re-uploads after a move");
    assert_eq!(ids(&network.hub.db, "notes"), vec![1]);
    let resent = occurrences(&network.hub.db)[hub_before.len()..].to_vec();
    assert_eq!(
        resent
            .iter()
            .map(|occurrence| (&occurrence.ddl, occurrence.author.clone()))
            .collect::<Vec<_>>(),
        held_at_move
            .iter()
            .filter(|occurrence| occurrence.lsn > since)
            .zip(&expected_resent)
            .map(|(occurrence, sent)| (&occurrence.ddl, sent.author.clone()))
            .collect::<Vec<_>>(),
        "the new hub records exactly what the edge held at the move, in order, each with its original author"
    );
    if !nightly {
        assert_one_epoch(
            "destination re-upload",
            &pushed.work,
            Some(moved.frontier),
            true,
        );
    }
    assert_sent("destination re-upload", &pushed.work, &expected_resent);
    if !nightly {
        assert_walk("destination re-upload", &pushed.work, &expected);
    }
    let sample = push_sample(size, &pushed, true);
    moved.network.close(&runtime);
    sample
}

#[test]
#[ignore = "CDB-235: an edge that pulled schema cannot re-upload after a move; the companion row pins the refusal"]
fn a_destination_reupload_resends_what_the_edge_held_and_holds_none_of_what_it_suppresses() {
    reupload_received_prefix(PR_SIZE, false);
}

#[cfg(feature = "nightly-tests")]
#[test]
#[ignore = "CDB-235: an edge that pulled schema cannot re-upload after a move; the companion row pins the refusal"]
fn a_destination_reupload_resends_what_the_edge_held_and_holds_none_of_what_it_suppresses_as_received_history_grows()
 {
    let measured = WALK_FROM_ZERO_SIZES.map(|size| reupload_received_prefix(size, true));
    let control = WALK_FROM_ZERO_SIZES
        .map(|size| control_sample(size, || reupload_received_prefix(PR_SIZE, true)));
    assert_walk_from_zero_push("destination re-upload", &measured, &control);
}

#[test]
fn a_destination_reupload_of_a_received_prefix_refuses_on_cdb_235_after_walking_exactly_its_history()
 {
    let _clock = fixed_clock();
    let runtime = runtime();
    let directory = tempfile::tempdir().unwrap();
    let Setup {
        moved, expected, ..
    } = received_prefix(&runtime, directory.path(), PR_SIZE);
    let network = &moved.network;
    let hub_before = occurrences(&network.hub.db);
    let pushed = measure(&network.edge.db, || runtime.block_on(network.client.push()));
    receipt("destination_reupload_cdb235", PR_SIZE, &pushed);
    match &pushed.value {
        Err(Error::SyncError(text)) if text.to_string().contains(CDB_235_REFUSAL) => {}
        other => panic!(
            "CDB-235 still refuses this re-upload with {CDB_235_REFUSAL:?}; got {other:?}. \
             If the re-upload now succeeds, un-ignore the full row above and delete this one."
        ),
    }
    assert_eq!(
        occurrences(&network.hub.db),
        hub_before,
        "a refused push sends nothing"
    );
    assert_one_epoch(
        "destination re-upload refused on CDB-235",
        &pushed.work,
        Some(moved.frontier),
        false,
    );
    assert_sent(
        "destination re-upload refused on CDB-235",
        &pushed.work,
        &[],
    );
    assert_walk(
        "destination re-upload refused on CDB-235",
        &pushed.work,
        &expected,
    );
    moved.network.close(&runtime);
}
