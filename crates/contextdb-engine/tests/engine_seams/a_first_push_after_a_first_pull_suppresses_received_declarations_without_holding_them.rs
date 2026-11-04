//! An edge whose first sync is a pull pushes from frontier 0 next. That first
//! push sends its own new row and none of the hub's declarations it received,
//! at the same memory cost however long the hub's schema history is.
use super::outbound_declaration_walk_support::*;
use contextdb_core::Lsn;

#[test]
fn a_first_push_after_a_first_pull_suppresses_received_declarations_without_holding_them() {
    let _clock = fixed_clock();
    let runtime = runtime();
    let mut samples = Vec::new();
    let mut works = Vec::new();
    for size in SIZES {
        let directory = tempfile::tempdir().unwrap();
        let edge = Peer::new(directory.path(), "edge", 0x11);
        let hub = Peer::new(directory.path(), "hub", 0x22);
        sql(
            &hub.db,
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) SYNC TWO WAY",
        );
        churn(&hub.db, "hub_history", size);
        sql(&hub.db, "INSERT INTO notes VALUES (1, 'first')");
        let network = Network::connect(&runtime, edge, hub);
        runtime.block_on(network.client.pull_default()).unwrap();
        assert_eq!(ids(&network.edge.db, "notes"), vec![1]);
        let received = occurrences(&network.edge.db)
            .iter()
            .filter(|occurrence| occurrence.received)
            .count() as u64;
        assert!(received > size as u64);
        assert_eq!(
            network.client.push_watermark(),
            Lsn(0),
            "the first push starts from frontier 0"
        );
        sql(&network.edge.db, "INSERT INTO notes VALUES (2, 'second')");
        let hub_schema = occurrences(&network.hub.db);
        let pushed = measure(&network.edge.db, || runtime.block_on(network.client.push()));
        receipt("first_push_after_first_pull", size, &pushed);
        assert_eq!(
            pushed
                .value
                .as_ref()
                .expect("the first push succeeds")
                .applied_rows,
            1
        );
        assert_eq!(occurrences(&network.hub.db), hub_schema);
        assert_eq!(ids(&network.hub.db, "notes"), vec![1, 2]);
        assert_eq!(pushed.work.suppressed, received);
        samples.push(comparable(&pushed));
        works.push((size, pushed.work));
        network.close(&runtime);
    }
    assert_same_across_sizes("first push after a first pull", &samples);
    for (size, work) in works {
        assert_suppression_holds_nothing("first push after a first pull", size, &work);
    }
}
