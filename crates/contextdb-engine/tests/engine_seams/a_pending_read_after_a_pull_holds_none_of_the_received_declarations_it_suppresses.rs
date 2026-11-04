//! Counting pending work right after a pull, before any push, reports no
//! received declaration, leaves the push frontier where it was, and costs the
//! same memory however many declarations were received. The push that follows
//! from that same frontier holds none of them either.
use super::outbound_declaration_walk_support::*;

#[test]
fn a_pending_read_after_a_pull_holds_none_of_the_received_declarations_it_suppresses() {
    let _clock = fixed_clock();
    let runtime = runtime();
    let mut pending_samples = Vec::new();
    let mut push_samples = Vec::new();
    let mut works = Vec::new();
    for size in SIZES {
        let directory = tempfile::tempdir().unwrap();
        let network = pulled_edge(&runtime, directory.path(), size, &|_| {});
        sql(&network.edge.db, "INSERT INTO notes VALUES (2, 'second')");
        let frontier = network.client.push_watermark();
        let pending = measure(&network.edge.db, || {
            network.client.pending_push_change_count()
        });
        receipt("pending_after_pull", size, &pending);
        assert_eq!(
            *pending.value.as_ref().expect("the pending read succeeds"),
            1,
            "the one new row is pending and no received declaration is"
        );
        assert_eq!(
            network.client.push_watermark(),
            frontier,
            "inspecting pending work leaves the push frontier unchanged"
        );
        assert_eq!(
            pending.work.reupload_epoch_reads, 1,
            "one epoch value per pending read"
        );
        let hub_schema = occurrences(&network.hub.db);
        let pushed = measure(&network.edge.db, || runtime.block_on(network.client.push()));
        receipt("push_after_pending", size, &pushed);
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
        pending_samples.push(comparable(&pending));
        push_samples.push(comparable(&pushed));
        works.push((size, pending.work, pushed.work));
        network.close(&runtime);
    }
    assert_same_across_sizes("pending read after a pull", &pending_samples);
    assert_same_across_sizes("push from the inspected frontier", &push_samples);
    for (size, pending, pushed) in works {
        assert_suppression_holds_nothing("pending read after a pull", size, &pending);
        assert_suppression_holds_nothing("push from the inspected frontier", size, &pushed);
    }
}
