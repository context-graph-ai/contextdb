//! A push never re-sends the declarations an edge received from its hub, and
//! suppressing them costs the same memory however many there are: none of
//! them is held in outbound output, and at most one is decoded at a time.
use super::outbound_declaration_walk_support::*;

#[test]
fn a_push_after_a_pull_holds_none_of_the_received_declarations_it_suppresses() {
    let _clock = fixed_clock();
    let runtime = runtime();
    let mut samples = Vec::new();
    let mut works = Vec::new();
    for size in SIZES {
        let directory = tempfile::tempdir().unwrap();
        let network = pulled_edge(&runtime, directory.path(), size, &|_| {});
        let received = received_groups(&network.edge.db);
        let received_occurrences = received.iter().map(|(_, n)| u64::from(*n)).sum::<u64>();
        assert!(
            received_occurrences as usize > size,
            "the pull marks the hub's declarations and the echoed notes declaration received: {received:?}"
        );
        sql(&network.edge.db, "INSERT INTO notes VALUES (2, 'second')");
        let hub_schema = occurrences(&network.hub.db);
        let pushed = measure(&network.edge.db, || runtime.block_on(network.client.push()));
        receipt("post_pull_push", size, &pushed);
        let applied = pushed.value.as_ref().expect("the post-pull push succeeds");
        assert_eq!(applied.applied_rows, 1, "the one new row reaches the hub");
        assert_eq!(
            occurrences(&network.hub.db),
            hub_schema,
            "a push after a pull sends no declaration back to the hub"
        );
        assert_eq!(ids(&network.hub.db, "notes"), vec![1, 2]);
        assert_eq!(
            pushed.work.suppressed, received_occurrences,
            "every received occurrence above the push frontier is suppressed"
        );
        assert_eq!(
            pushed.work.reupload_epoch_reads, 1,
            "one epoch value per push"
        );
        samples.push(comparable(&pushed));
        works.push((size, pushed.work));
        network.close(&runtime);
    }
    assert_same_across_sizes("post-pull push", &samples);
    for (size, work) in works {
        assert_suppression_holds_nothing("post-pull push", size, &work);
    }
}
