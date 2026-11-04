//! A push sends every local declaration group after its frontier, whole and in
//! order, including groups on both sides of received groups it drops and a
//! group holding two equal declarations at different ordinals. Each sent
//! occurrence keeps its source LSN, ordinal and the edge as its author. Local
//! groups already superseded by received schema are not sent: a single
//! declaration and a whole multi-statement commit. A received multi-statement
//! group that is also fully superseded is valid and dropped like any other
//! received group. Nightly, the push costs the same however much received
//! history sits between the local groups.
#![expect(
    clippy::result_large_err,
    reason = "The measured operations return the public Error, which keeps inline NativeRefusal diagnostics."
)]
use super::outbound_declaration_walk_support::*;
use contextdb_core::Lsn;
use contextdb_engine::database::DdlEvidenceEditForTest;

fn mark_superseded(db: &contextdb_engine::Database, lsn: Lsn) {
    for occurrence in occurrences(db)
        .into_iter()
        .filter(|occurrence| occurrence.lsn == lsn)
    {
        db.edit_ddl_occurrence_evidence_for_test(
            occurrence.lsn,
            occurrence.ordinal,
            DdlEvidenceEditForTest::MarkSuperseded,
        )
        .unwrap();
    }
}

fn last_lsn_naming(db: &contextdb_engine::Database, prefix: &str) -> Lsn {
    occurrences(db)
        .into_iter()
        .rev()
        .find(|occurrence| names(&occurrence.ddl, prefix))
        .unwrap_or_else(|| panic!("the edge holds {prefix}"))
        .lsn
}

fn local_groups_around_suppressed(size: usize, nightly: bool) -> (usize, Lsn, usize) {
    let _clock = fixed_clock();
    let runtime = runtime();
    let directory = tempfile::tempdir().unwrap();
    let network = pulled_edge(
        &runtime,
        directory.path(),
        size,
        nightly_pad(size, nightly),
        &|edge| {
            sql(
                edge,
                "CREATE TABLE before_pull (id INTEGER PRIMARY KEY) SYNC TWO WAY",
            );
        },
    );
    let edge = &network.edge.db;
    let frontier = network.client.push_watermark();
    let (received_unit, unit_members) = last_multi_occurrence_received_group(edge);
    mark_superseded(edge, received_unit);
    one_commit(
        edge,
        &[
            "CREATE TABLE twin (id INTEGER PRIMARY KEY) SYNC TWO WAY".to_string(),
            "ALTER TABLE twin ADD COLUMN spare INTEGER".to_string(),
            "ALTER TABLE twin DROP COLUMN spare".to_string(),
            "ALTER TABLE twin ADD COLUMN spare INTEGER".to_string(),
        ],
    );
    sql(
        edge,
        "CREATE TABLE settled (id INTEGER PRIMARY KEY) SYNC OFF",
    );
    mark_superseded(edge, last_lsn_naming(edge, "settled"));
    one_commit(edge, &large_commit("settled_whole", 3));
    mark_superseded(edge, last_lsn_naming(edge, "settled_whole"));
    let schema = occurrences(edge);
    let unit = schema
        .iter()
        .filter(|occurrence| occurrence.lsn == received_unit)
        .collect::<Vec<_>>();
    assert!(
        unit.len() == unit_members as usize
            && unit.len() > 1
            && unit
                .iter()
                .all(|occurrence| occurrence.received && occurrence.superseded),
        "the hub's unit is a received group of several occurrences, all also superseded: {unit:?}"
    );
    let settled_whole = schema
        .iter()
        .filter(|occurrence| names(&occurrence.ddl, "settled_whole"))
        .collect::<Vec<_>>();
    assert!(
        settled_whole.len() == 3
            && settled_whole
                .iter()
                .all(|occurrence| occurrence.lsn == settled_whole[0].lsn
                    && occurrence.superseded
                    && !occurrence.received),
        "a local commit of three declarations, all superseded: {settled_whole:?}"
    );
    let sent_expected = schema
        .iter()
        .filter(|occurrence| {
            occurrence.lsn > frontier && !occurrence.received && !occurrence.superseded
        })
        .cloned()
        .collect::<Vec<_>>();
    let twin = sent_expected
        .iter()
        .filter(|occurrence| names(&occurrence.ddl, "twin"))
        .collect::<Vec<_>>();
    assert_eq!(twin.len(), 4, "the twin commit is one group of four");
    assert!(twin.iter().all(|occurrence| occurrence.lsn == twin[0].lsn));
    assert_eq!(
        twin[1].ddl, twin[3].ddl,
        "two equal declarations in one group"
    );
    let suppressed_between = schema
        .iter()
        .filter(|occurrence| {
            occurrence.received
                && occurrence.lsn > sent_expected[0].lsn
                && occurrence.lsn < twin[0].lsn
        })
        .count();
    assert!(
        suppressed_between > size,
        "received groups sit between the two local groups"
    );
    let expected = expected_walk(&schema, frontier, None);
    assert!(
        unit.iter().all(|occurrence| expected
            .dropped
            .contains(&(occurrence.lsn, occurrence.ordinal))),
        "a received group is dropped whether or not it is also superseded"
    );
    let hub_before = occurrences(&network.hub.db);
    let pushed = measure(edge, || runtime.block_on(network.client.push()));
    receipt("local_groups_around_suppressed", size, &pushed);
    pushed.value.as_ref().expect("the push succeeds");
    let sent = occurrences(&network.hub.db)[hub_before.len()..].to_vec();
    let edge_node = network.edge.node_id();
    assert_eq!(
        sent.iter()
            .map(|occurrence| &occurrence.ddl)
            .collect::<Vec<_>>(),
        sent_expected
            .iter()
            .map(|occurrence| &occurrence.ddl)
            .collect::<Vec<_>>(),
        "exactly the local groups after the frontier are sent, in order, without the superseded ones"
    );
    assert_eq!(
        sent.iter()
            .map(|occurrence| occurrence.author.clone())
            .collect::<Vec<_>>(),
        sent_expected
            .iter()
            .map(|occurrence| Some((edge_node.clone(), occurrence.lsn, occurrence.ordinal)))
            .collect::<Vec<_>>(),
        "each sent occurrence keeps its source LSN and ordinal and names the edge as author"
    );
    if !nightly {
        assert_one_epoch(
            "local groups around suppressed ones",
            &pushed.work,
            None,
            true,
        );
    }
    assert_sent(
        "local groups around suppressed ones",
        &pushed.work,
        &sent_as_local(&edge_node, &sent_expected),
    );
    if !nightly {
        assert_walk(
            "local groups around suppressed ones",
            &pushed.work,
            &expected,
        );
    }
    let sample = (size, pushed.lsn, pushed.peak);
    network.close(&runtime);
    sample
}

#[test]
fn a_push_sends_local_groups_around_suppressed_ones_with_their_ordinals_and_skips_superseded_ones()
{
    local_groups_around_suppressed(PR_SIZE, false);
}

#[cfg(feature = "nightly-tests")]
#[test]
fn a_push_sends_local_groups_around_suppressed_ones_with_their_ordinals_and_skips_superseded_ones_as_received_history_grows()
 {
    let measured = NIGHTLY_SIZES.map(|size| local_groups_around_suppressed(size, true));
    let control = NIGHTLY_SIZES
        .map(|size| control_sample(size, || local_groups_around_suppressed(PR_SIZE, true)));
    assert_bounded_by_control(
        "local groups around suppressed ones",
        &measured,
        &control,
        BallastIn::NoWalk,
    );
}
