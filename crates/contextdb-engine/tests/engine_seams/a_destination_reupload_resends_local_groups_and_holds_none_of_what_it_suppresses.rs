//! After an edge moves to a new hub, its re-upload resends every local
//! declaration group it held at the move, whole and in order, each naming the
//! edge as author with its source LSN and ordinal, and suppresses every
//! declaration it received after the move. The resent groups are fixed while
//! the suppressed suffix grows, and the push costs the same memory at both
//! sizes. One epoch value serves the whole push.
use super::outbound_declaration_walk_support::*;

#[test]
fn a_destination_reupload_resends_local_groups_and_holds_none_of_what_it_suppresses() {
    let _clock = fixed_clock();
    let runtime = runtime();
    let mut samples = Vec::new();
    let mut works = Vec::new();
    let mut resent_by_size = Vec::new();
    for size in SIZES {
        let directory = tempfile::tempdir().unwrap();
        let moved = moved_edge(
            &runtime,
            directory.path(),
            size,
            Prefix::AuthoredByTheEdge,
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
        let received_after_move = edge_schema
            .iter()
            .filter(|occurrence| occurrence.lsn > moved.frontier && occurrence.received)
            .count() as u64;
        assert_eq!(
            received_after_move, size as u64,
            "the second hub's declarations arrive above F"
        );
        let hub_before = occurrences(&network.hub.db);
        let pushed = measure(&network.edge.db, || runtime.block_on(network.client.push()));
        receipt("destination_reupload_local", size, &pushed);
        pushed.value.as_ref().expect("the re-upload succeeds");
        assert_eq!(ids(&network.hub.db, "notes"), vec![1]);
        let resent = occurrences(&network.hub.db)[hub_before.len()..].to_vec();
        let edge_node = network.edge.node_id();
        assert_eq!(
            resent
                .iter()
                .map(|occurrence| &occurrence.ddl)
                .collect::<Vec<_>>(),
            held_at_move
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
            held_at_move
                .iter()
                .map(|occurrence| Some((edge_node.clone(), occurrence.lsn, occurrence.ordinal)))
                .collect::<Vec<_>>(),
            "each resent occurrence names the edge as author with its source LSN and ordinal"
        );
        assert_eq!(
            pushed.work.suppressed, received_after_move,
            "every declaration received after the move is suppressed"
        );
        assert_eq!(
            pushed.work.reupload_epoch_reads, 1,
            "one epoch value per push"
        );
        resent_by_size.push(
            resent
                .iter()
                .map(|occurrence| occurrence.ddl.clone())
                .collect::<Vec<_>>(),
        );
        samples.push(comparable(&pushed));
        works.push((size, pushed.work));
        moved.network.close(&runtime);
    }
    assert_eq!(
        resent_by_size[0], resent_by_size[1],
        "the resent groups are the same at both sizes"
    );
    assert_same_across_sizes("destination re-upload of local groups", &samples);
    for (size, work) in works {
        assert_suppression_holds_nothing("destination re-upload of local groups", size, &work);
    }
}
