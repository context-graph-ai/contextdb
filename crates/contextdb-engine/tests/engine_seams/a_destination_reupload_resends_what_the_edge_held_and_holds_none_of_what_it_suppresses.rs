//! After an edge moves to a new hub, its re-upload resends every declaration
//! it held at the move, local and received alike, each with its original
//! author, and suppresses what it received after the move. The resent prefix
//! is fixed while the suppressed suffix grows, and the push costs the same
//! memory at both sizes. One epoch value serves the whole push.
//!
//! Known separate defect: today an edge that ever pulled schema cannot
//! re-upload after a move at all. The push refuses with "table notes has
//! schema metadata but no authoritative generation", wrongly reported as a
//! corrupt store, so this row fails at the re-upload until that is fixed. The
//! row beside it, with a locally authored prefix, proves the suppression
//! promise without depending on that fix.
use super::outbound_declaration_walk_support::*;

#[test]
fn a_destination_reupload_resends_what_the_edge_held_and_holds_none_of_what_it_suppresses() {
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
            Prefix::ReceivedFromTheFirstHub,
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
            held_at_move
                .iter()
                .any(|occurrence| occurrence.received && occurrence.lsn == moved.frontier),
            "the pull just before the move leaves a received group exactly at F"
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
        receipt("destination_reupload", size, &pushed);
        pushed
            .value
            .as_ref()
            .expect("an edge holding received schema re-uploads after a move");
        assert_eq!(ids(&network.hub.db, "notes"), vec![1]);
        let hub_after = occurrences(&network.hub.db);
        let resent = hub_after[hub_before.len()..].to_vec();
        for occurrence in &resent {
            eprintln!(
                "reupload size={size} resent lsn={} ordinal={} author={:?} ddl={:?}",
                occurrence.lsn.0, occurrence.ordinal, occurrence.author, occurrence.ddl
            );
        }
        assert!(
            resent
                .iter()
                .all(|occurrence| !names(&occurrence.ddl, "moved_")),
            "a declaration received after the move is never resent"
        );
        let prefix = resent
            .iter()
            .filter(|occurrence| names(&occurrence.ddl, "prefix_a"))
            .collect::<Vec<_>>();
        assert_eq!(
            prefix.len(),
            2,
            "both received prefix declarations are resent"
        );
        assert!(
            prefix.iter().all(|occurrence| occurrence
                .author
                .as_ref()
                .is_some_and(|(node, _, _)| *node == moved.first_hub_node)),
            "a resent received declaration keeps its author, the first hub"
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
                .map(|occurrence| (occurrence.ddl.clone(), occurrence.author.clone()))
                .collect::<Vec<_>>(),
        );
        samples.push(comparable(&pushed));
        works.push((size, pushed.work));
        moved.network.close(&runtime);
    }
    assert_eq!(
        resent_by_size[0], resent_by_size[1],
        "the resent prefix is the same at both sizes"
    );
    assert_same_across_sizes("destination re-upload", &samples);
    for (size, work) in works {
        assert_suppression_holds_nothing("destination re-upload", size, &work);
    }
}
