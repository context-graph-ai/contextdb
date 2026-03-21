/// I worked offline for a long time accumulating 10,000 rows, reconnected, and all of them made it to the server.
#[test]
fn f25_edge_offline_accumulates_ten_thousand_rows_reconnects_and_pushes() {
    assert!(false, "performance benchmark - run manually");
}

/// I set up a fresh edge and pulled 10,000 rows from the server, and every single row arrived.
#[tokio::test]
async fn f26_large_pull_ten_thousand_rows_from_server_to_fresh_edge() {
    panic!("fresh edge should receive all 10,000 rows from the server");
}

/// I did an initial sync, then the server got more data, and pulling again gave me only the new rows — not the whole dataset again.
#[tokio::test]
async fn f27_incremental_pull_after_initial_sync() {
    panic!("incremental pull should deliver exactly the post-baseline delta");
}
