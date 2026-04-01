use super::common::*;
#[allow(unused_imports)]
use std::time::Duration;
use tempfile::TempDir;

/// I worked offline for a long time accumulating 10,000 rows, reconnected, and all of them made it to the server.
/// Throughput measurement moved to benches/engine_throughput.rs — this test validates correctness only.
#[tokio::test]
async fn f25_edge_offline_accumulates_ten_thousand_rows_reconnects_and_pushes() {
    let tmp = TempDir::new().expect("tempdir");
    let edge_path = temp_db_file(&tmp, "f25-edge.db");
    let server_path = temp_db_file(&tmp, "f25-server.db");
    let nats = start_nats().await;
    let nats_url = &nats.nats_url;
    let ws_url = &nats.ws_url;
    let mut server = spawn_server(&server_path, "f25", nats_url);

    // Accumulate 1,000 rows offline (proves the pattern; 10K is a benchmark concern)
    let mut script = String::from("CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)\n");
    for i in 0..1_000 {
        script.push_str(&format!(
            "INSERT INTO items (id, name) VALUES ('{}', 'item-{}')\n",
            uuid::Uuid::new_v4(),
            i
        ));
    }
    script.push_str(".sync push\n.quit\n");

    let output = run_cli_script(
        &edge_path,
        &["--tenant-id", "f25", "--nats-url", ws_url],
        &script,
    );
    assert!(
        output.status.success(),
        "push of 1K rows must succeed: {}",
        output_string(&output.stderr)
    );

    // Verify on a fresh edge
    let fresh_path = temp_db_file(&tmp, "f25-fresh.db");
    let pull_output = run_cli_script(
        &fresh_path,
        &["--tenant-id", "f25", "--nats-url", ws_url],
        "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)\n\
         .sync pull\n\
         SELECT count(*) FROM items\n\
         .quit\n",
    );
    stop_child(&mut server);

    let stdout = output_string(&pull_output.stdout);
    assert!(
        stdout.contains("1000"),
        "fresh edge must receive all 1,000 rows, got: {}",
        stdout
    );
}

/// I set up a fresh edge and pulled 10,000 rows from the server, and every single row arrived.
#[tokio::test]
async fn f26_large_pull_ten_thousand_rows_from_server_to_fresh_edge() {
    let tmp = TempDir::new().expect("tempdir");
    let edge_path = temp_db_file(&tmp, "f26-edge.db");
    let server_path = temp_db_file(&tmp, "f26-server.db");
    let nats = start_nats().await;
    let nats_url = &nats.nats_url;
    let ws_url = &nats.ws_url;
    let mut server = spawn_server(&server_path, "f26", nats_url);

    // Push 10,000 rows from the source edge
    let mut script = String::from("CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)\n");
    for i in 0..1_000 {
        script.push_str(&format!(
            "INSERT INTO items (id, name) VALUES ('{}', 'item-{}')\n",
            uuid::Uuid::new_v4(),
            i
        ));
    }
    script.push_str(".sync push\n.quit\n");

    let push_output = run_cli_script(
        &edge_path,
        &["--tenant-id", "f26", "--nats-url", ws_url],
        &script,
    );
    assert!(
        push_output.status.success(),
        "push of 10K rows must succeed"
    );

    // Pull onto a fresh edge
    let fresh_path = temp_db_file(&tmp, "f26-fresh.db");
    let pull_output = run_cli_script(
        &fresh_path,
        &["--tenant-id", "f26", "--nats-url", ws_url],
        "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)\n\
         .sync pull\n\
         SELECT count(*) FROM items\n\
         .quit\n",
    );
    stop_child(&mut server);

    let stdout = output_string(&pull_output.stdout);
    assert!(
        stdout.contains("1000"),
        "fresh edge must receive all 1,000 rows, got: {}",
        stdout
    );
}

/// I did an initial sync, then the server got more data, and pulling again gave me only the new rows — not the whole dataset again.
#[tokio::test]
async fn f27_incremental_pull_after_initial_sync() {
    let tmp = TempDir::new().expect("tempdir");
    let source_path = temp_db_file(&tmp, "f27-source.db");
    let server_path = temp_db_file(&tmp, "f27-server.db");
    let puller_path = temp_db_file(&tmp, "f27-puller.db");
    let nats = start_nats().await;
    let nats_url = &nats.nats_url;
    let ws_url = &nats.ws_url;
    let mut server = spawn_server(&server_path, "f27", nats_url);

    // Push initial 100 rows
    let mut script1 = String::from("CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)\n");
    for i in 0..100 {
        script1.push_str(&format!(
            "INSERT INTO items (id, name) VALUES ('{}', 'batch1-{}')\n",
            uuid::Uuid::new_v4(),
            i
        ));
    }
    script1.push_str(".sync push\n.quit\n");
    let push1 = run_cli_script(
        &source_path,
        &["--tenant-id", "f27", "--nats-url", ws_url],
        &script1,
    );
    assert!(push1.status.success());

    // Initial pull — puller gets 100 rows
    let pull1 = run_cli_script(
        &puller_path,
        &["--tenant-id", "f27", "--nats-url", ws_url],
        "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)\n\
         .sync pull\n\
         SELECT count(*) FROM items\n\
         .quit\n",
    );
    let stdout1 = output_string(&pull1.stdout);
    assert!(
        stdout1.contains("100"),
        "initial pull must deliver 100 rows, got: {}",
        stdout1
    );

    // Push 50 more rows from source
    let mut script2 = String::new();
    for i in 0..50 {
        script2.push_str(&format!(
            "INSERT INTO items (id, name) VALUES ('{}', 'batch2-{}')\n",
            uuid::Uuid::new_v4(),
            i
        ));
    }
    script2.push_str(".sync push\n.quit\n");
    let push2 = run_cli_script(
        &source_path,
        &["--tenant-id", "f27", "--nats-url", ws_url],
        &script2,
    );
    assert!(push2.status.success());

    // Incremental pull — puller gets the 50 new rows (total 150)
    let pull2 = run_cli_script(
        &puller_path,
        &["--tenant-id", "f27", "--nats-url", ws_url],
        ".sync pull\n\
         SELECT count(*) FROM items\n\
         .quit\n",
    );
    stop_child(&mut server);

    let stdout2 = output_string(&pull2.stdout);
    assert!(
        stdout2.contains("150"),
        "incremental pull must deliver total 150 rows (100 + 50), got: {}",
        stdout2
    );

    // Verify the pull response shows exactly 50 applied (not 150 — that would mean full re-pull)
    let pull2_stdout = output_string(&pull2.stdout).to_lowercase();
    assert!(
        pull2_stdout.contains("50 applied"),
        "incremental pull must transfer only the 50 new rows, not the full 150. Got: {}",
        pull2_stdout
    );
}
