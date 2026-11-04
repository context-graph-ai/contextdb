//! A push sends every local declaration group after its frontier, whole and in
//! order, including groups on both sides of a received group it suppresses
//! and a group holding two equal declarations at different ordinals. Each sent
//! occurrence keeps its source LSN, ordinal and the edge as its author. A
//! local group already superseded by received schema is not sent. The cost is
//! the same however much received history sits between the local groups.
use super::outbound_declaration_walk_support::*;
use contextdb_engine::database::DdlEvidenceEditForTest;

#[test]
fn a_push_sends_local_groups_around_suppressed_ones_with_their_ordinals_and_skips_superseded_ones()
{
    let _clock = fixed_clock();
    let runtime = runtime();
    let mut samples = Vec::new();
    let mut works = Vec::new();
    let mut sent_by_size = Vec::new();
    for size in SIZES {
        let directory = tempfile::tempdir().unwrap();
        let network = pulled_edge(&runtime, directory.path(), size, &|edge| {
            sql(
                edge,
                "CREATE TABLE before_pull (id INTEGER PRIMARY KEY) SYNC TWO WAY",
            );
        });
        let edge = &network.edge.db;
        let frontier = network.client.push_watermark();
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
        let settled = occurrences(edge)
            .into_iter()
            .rev()
            .find(|occurrence| names(&occurrence.ddl, "settled"))
            .unwrap();
        edge.edit_ddl_occurrence_evidence_for_test(
            settled.lsn,
            settled.ordinal,
            DdlEvidenceEditForTest::MarkSuperseded,
        )
        .unwrap();
        let schema = occurrences(edge);
        let expected = schema
            .iter()
            .filter(|occurrence| {
                occurrence.lsn > frontier && !occurrence.received && !occurrence.superseded
            })
            .cloned()
            .collect::<Vec<_>>();
        let twin = expected
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
                    && occurrence.lsn > expected[0].lsn
                    && occurrence.lsn < twin[0].lsn
            })
            .count();
        assert!(
            suppressed_between > size,
            "received groups sit between the two local groups"
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
            expected
                .iter()
                .map(|occurrence| &occurrence.ddl)
                .collect::<Vec<_>>(),
            "exactly the local groups after the frontier are sent, in order, without the superseded one"
        );
        assert_eq!(
            sent.iter()
                .map(|occurrence| occurrence.author.clone())
                .collect::<Vec<_>>(),
            expected
                .iter()
                .map(|occurrence| Some((edge_node.clone(), occurrence.lsn, occurrence.ordinal)))
                .collect::<Vec<_>>(),
            "each sent occurrence keeps its source LSN and ordinal and names the edge as author"
        );
        sent_by_size.push(
            sent.iter()
                .map(|occurrence| occurrence.ddl.clone())
                .collect::<Vec<_>>(),
        );
        samples.push(comparable(&pushed));
        works.push((size, pushed.work));
        network.close(&runtime);
    }
    assert_eq!(sent_by_size[0], sent_by_size[1]);
    assert_same_across_sizes("local groups around suppressed ones", &samples);
    for (size, work) in works {
        assert_suppression_holds_nothing("local groups around suppressed ones", size, &work);
    }
}
