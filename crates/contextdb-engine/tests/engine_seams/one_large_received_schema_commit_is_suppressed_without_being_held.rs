//! One received schema commit of many declarations lands as one local group.
//! A push drops the whole group as it walks: every one of its declarations is
//! dropped, none reaches the final filter, and nightly the push costs the same
//! memory whether the group holds 16 declarations or 1,024.
#![expect(
    clippy::result_large_err,
    reason = "The measured operations return the public Error, which keeps inline NativeRefusal diagnostics."
)]
use super::outbound_declaration_walk_support::*;
use contextdb_core::Lsn;

fn large_received_group(size: usize, nightly: bool) -> (usize, Lsn, usize) {
    let _clock = fixed_clock();
    let runtime = runtime();
    let directory = tempfile::tempdir().unwrap();
    let edge = Peer::new(directory.path(), "edge", 0x11);
    let hub = Peer::new(directory.path(), "hub", 0x22);
    sql(
        &edge.db,
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) SYNC TWO WAY",
    );
    if in_control_sample() {
        sql(
            &edge.db,
            "CREATE TABLE store_ballast (id INTEGER PRIMARY KEY) SYNC OFF",
        );
        store_ballast(&edge.db, "store_ballast");
    }
    sql(&edge.db, "INSERT INTO notes VALUES (1, 'first')");
    let network = Network::connect(&runtime, edge, hub);
    runtime.block_on(network.client.push()).unwrap();
    one_commit(&network.hub.db, &large_commit("bulk", size));
    runtime.block_on(network.client.pull_default()).unwrap();
    let history = occurrences(&network.edge.db);
    let bulk = history
        .iter()
        .filter(|occurrence| names(&occurrence.ddl, "bulk_"))
        .map(|occurrence| (occurrence.lsn, occurrence.ordinal, occurrence.received))
        .collect::<Vec<_>>();
    assert_eq!(bulk.len(), size, "every declaration of the commit arrives");
    assert_eq!(
        bulk,
        (0..size as u32)
            .map(|ordinal| (bulk[0].0, ordinal, true))
            .collect::<Vec<_>>(),
        "the received commit is one local group at one source LSN, ordinals 0..{size}"
    );
    sql(&network.edge.db, "INSERT INTO notes VALUES (2, 'second')");
    let expected = expected_walk(&history, network.client.push_watermark(), None);
    let bulk_occurrences = bulk
        .iter()
        .map(|(lsn, ordinal, _)| (*lsn, *ordinal))
        .collect::<Vec<_>>();
    assert!(
        expected
            .dropped
            .iter()
            .filter(|(lsn, _)| *lsn == bulk[0].0)
            .eq(bulk_occurrences.iter()),
        "every occurrence of the large group is droppable: {expected:?}"
    );
    let hub_schema = occurrences(&network.hub.db);
    let pushed = measure(&network.edge.db, || runtime.block_on(network.client.push()));
    receipt("large_received_group", size, &pushed);
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
        assert_one_epoch("one large received group", &pushed.work, None, true);
    }
    assert_sent("one large received group", &pushed.work, &[]);
    if !nightly {
        assert_walk("one large received group", &pushed.work, &expected);
    }
    let sample = (size, pushed.lsn, pushed.peak);
    network.close(&runtime);
    sample
}

#[test]
fn one_large_received_schema_commit_is_suppressed_without_being_held() {
    large_received_group(PR_SIZE, false);
}

#[cfg(feature = "nightly-tests")]
#[test]
fn one_large_received_schema_commit_is_suppressed_without_being_held_as_received_history_grows() {
    let measured = NIGHTLY_SIZES.map(|size| large_received_group(size, true));
    let control =
        NIGHTLY_SIZES.map(|size| control_sample(size, || large_received_group(PR_SIZE, true)));
    assert_bounded_by_control(
        "one large received group",
        &measured,
        &control,
        BallastIn::NoWalk,
    );
}
