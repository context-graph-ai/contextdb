//! Closing SYNC before rewriting a key must not expose its older eligible value.
use super::outbound_declaration_walk_support::*;
use contextdb_core::Value;
use std::collections::HashMap;

#[test]
fn a_push_never_sends_an_older_key_version_after_sync_closes() {
    let _clock = fixed_clock();
    let runtime = runtime();
    let directory = tempfile::tempdir().unwrap();
    let edge = Peer::new(directory.path(), "edge", 1);
    let hub = Peer::new(directory.path(), "hub", 2);
    sql(
        &edge.db,
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) SYNC TWO WAY",
    );
    let network = Network::connect(&runtime, edge, hub);
    runtime.block_on(network.client.push()).unwrap();
    let frontier = network.client.push_watermark();
    sql(&network.edge.db, "INSERT INTO notes VALUES (1, 'eligible')");
    let eligible = network.edge.db.current_lsn();
    sql(&network.edge.db, "ALTER TABLE notes SET SYNC OFF");
    let closed = network.edge.db.current_lsn();
    sql(
        &network.edge.db,
        "UPDATE notes SET body = 'closed' WHERE id = 1",
    );
    let rewritten = network.edge.db.current_lsn();
    assert!(frontier < eligible && eligible < closed && closed < rewritten);
    assert_eq!(network.client.push_watermark(), frontier);
    assert_eq!(ids(&network.edge.db, "notes"), vec![1]);
    assert_eq!(
        network
            .edge
            .db
            .execute("SELECT body FROM notes WHERE id = 1", &HashMap::new())
            .unwrap()
            .rows(),
        vec![vec![Value::Text("closed".into())]],
        "the latest version really was rewritten after SYNC closed"
    );
    assert!(ids(&network.hub.db, "notes").is_empty());

    let pushed = runtime.block_on(network.client.push());
    let applied =
        pushed.expect("a closed latest version must suppress the key without a push error");
    assert_eq!(
        applied.applied_rows, 0,
        "no version of the closed key may be applied"
    );
    assert!(
        ids(&network.hub.db, "notes").is_empty(),
        "filtering a closed latest version must never resurrect its older eligible value"
    );
    network.close(&runtime);
}
