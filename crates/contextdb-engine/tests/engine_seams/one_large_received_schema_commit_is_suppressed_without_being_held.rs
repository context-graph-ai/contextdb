//! One received schema commit of many declarations lands as one local group,
//! and a push suppresses it at the same memory cost whether it holds 16
//! declarations or 1,024: none of them is held in outbound output.
use super::outbound_declaration_walk_support::*;

#[test]
fn one_large_received_schema_commit_is_suppressed_without_being_held() {
    let _clock = fixed_clock();
    let runtime = runtime();
    let mut samples = Vec::new();
    let mut works = Vec::new();
    for size in SIZES {
        let directory = tempfile::tempdir().unwrap();
        let edge = Peer::new(directory.path(), "edge", 0x11);
        let hub = Peer::new(directory.path(), "hub", 0x22);
        sql(
            &edge.db,
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) SYNC TWO WAY",
        );
        sql(&edge.db, "INSERT INTO notes VALUES (1, 'first')");
        let network = Network::connect(&runtime, edge, hub);
        runtime.block_on(network.client.push()).unwrap();
        one_commit(&network.hub.db, &large_commit("bulk", size));
        runtime.block_on(network.client.pull_default()).unwrap();
        let bulk = occurrences(&network.edge.db)
            .into_iter()
            .filter(|occurrence| names(&occurrence.ddl, "bulk_"))
            .collect::<Vec<_>>();
        assert_eq!(bulk.len(), size, "every declaration of the commit arrives");
        assert!(
            bulk.iter()
                .all(|occurrence| occurrence.lsn == bulk[0].lsn && occurrence.received),
            "the received commit is one local group at one source LSN"
        );
        let largest = received_groups(&network.edge.db)
            .iter()
            .map(|(_, members)| *members)
            .max()
            .unwrap();
        eprintln!(
            "large_group size={size} largest_local_group={largest} local_lsn={}",
            bulk[0].lsn.0
        );
        sql(&network.edge.db, "INSERT INTO notes VALUES (2, 'second')");
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
        assert!(pushed.work.suppressed >= size as u64, "{:?}", pushed.work);
        samples.push(comparable(&pushed));
        works.push((size, pushed.work));
        network.close(&runtime);
    }
    assert_same_across_sizes("one large received group", &samples);
    for (size, work) in works {
        assert_suppression_holds_nothing("one large received group", size, &work);
    }
}
