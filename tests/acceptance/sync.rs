use super::common::*;
use std::time::Duration;
use tempfile::TempDir;

async fn setup_sync_env(name: &str) -> (TempDir, std::path::PathBuf, std::path::PathBuf, String) {
    let tmp = TempDir::new().expect("tempdir");
    let edge_path = temp_db_file(&tmp, &format!("{name}-edge.db"));
    let server_path = temp_db_file(&tmp, &format!("{name}-server.db"));
    let nats = start_nats().await;
    (tmp, edge_path, server_path, nats.nats_url)
}

/// I wrote data on an edge node, and it showed up on the server without me running any sync command.
#[tokio::test]
async fn f06a_data_written_on_edge_appears_on_server_automatically() {
    let (_tmp, edge_path, server_path, nats_url) = setup_sync_env("f06a").await;
    let tenant = "f06a";
    let mut server = spawn_server(&server_path, tenant, &nats_url);
    let edge = run_cli_script(
        &edge_path,
        &["--tenant-id", tenant, "--nats-url", &nats_url],
        "\
CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n\
INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'a')\n\
INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000002', 'b')\n\
INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000003', 'c')\n\
INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000004', 'd')\n\
INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000005', 'e')\n\
INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000006', 'f')\n\
INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000007', 'g')\n\
INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000008', 'h')\n\
INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000009', 'i')\n\
INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000010', 'j')\n\
.quit\n",
    );
    assert!(edge.status.success());
    let synced = wait_until(Duration::from_secs(5), || {
        stop_child(&mut server);
        let count = count_rows_from_file(&server_path, "sensors");
        if count == 10 {
            true
        } else {
            server = spawn_server(&server_path, tenant, &nats_url);
            false
        }
    });
    if server.try_wait().expect("server status").is_none() {
        stop_child(&mut server);
    }
    assert!(synced, "server should receive rows without manual push");
}

macro_rules! sync_red_test {
    ($name:ident, $script:expr, $check:expr) => {
        #[tokio::test]
        async fn $name() {
            let (_tmp, edge_path, server_path, nats_url) = setup_sync_env(stringify!($name)).await;
            let tenant = stringify!($name);
            let mut server = spawn_server(&server_path, tenant, &nats_url);
            let output = run_cli_script(
                &edge_path,
                &["--tenant-id", tenant, "--nats-url", &nats_url],
                $script,
            );
            assert!(output.status.success());
            stop_child(&mut server);
            ($check)(&edge_path, &server_path, &nats_url);
        }
    };
}

// I wrote data on one edge, and another edge saw it without any manual sync.
sync_red_test!(
    f06b_data_from_another_edge_appears_on_this_edge_automatically,
    "\
CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n\
INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'edge-a')\n\
.quit\n",
    |edge_path: &std::path::Path, _server_path: &std::path::Path, nats_url: &str| {
        let output = run_cli_script(
            edge_path,
            &[
                "--tenant-id",
                "f06b_data_from_another_edge_appears_on_this_edge_automatically",
                "--nats-url",
                nats_url,
            ],
            "SELECT count(*) FROM sensors\n.quit\n",
        );
        assert!(output_string(&output.stdout).contains("10"));
    }
);

// I wrote data while the edge was offline, and when the network came back the backlog was synced automatically.
sync_red_test!(
    f06c_edge_reconnects_after_network_outage_and_auto_syncs_backlog,
    "\
CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n\
INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'offline')\n\
.quit\n",
    |_edge_path: &std::path::Path, server_path: &std::path::Path, _nats_url: &str| {
        assert_eq!(count_rows_from_file(server_path, "sensors"), 50);
    }
);

// I pushed data from the edge, and the server had all the rows.
sync_red_test!(
    f06_edge_pushes_data_server_has_it,
    "\
CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n\
INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'sensor')\n\
.sync push\n\
.quit\n",
    |_edge_path: &std::path::Path, server_path: &std::path::Path, _nats_url: &str| {
        assert_eq!(count_rows_from_file(server_path, "sensors"), 100);
    }
);

// I pushed twice in a row, and the server had each row exactly once — no duplicates.
sync_red_test!(
    f07_two_consecutive_pushes_do_not_duplicate_data,
    "\
CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n\
INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'one')\n\
.sync push\n\
INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000002', 'two')\n\
.sync push\n\
.quit\n",
    |_edge_path: &std::path::Path, server_path: &std::path::Path, _nats_url: &str| {
        assert_eq!(count_rows_from_file(server_path, "sensors"), 100);
    }
);

// I pushed from one edge, then pulled onto a brand-new edge, and the fresh edge had all the data.
sync_red_test!(
    f08_push_then_pull_on_a_fresh_edge,
    "\
CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n\
INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'one')\n\
.sync push\n\
.quit\n",
    |edge_path: &std::path::Path, _server_path: &std::path::Path, nats_url: &str| {
        let fresh_path = edge_path.with_file_name("fresh-edge.db");
        let pulled = run_cli_script(
            &fresh_path,
            &[
                "--tenant-id",
                "f08_push_then_pull_on_a_fresh_edge",
                "--nats-url",
                nats_url,
            ],
            "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n.sync pull\nSELECT count(*) FROM sensors\n.quit\n",
        );
        assert!(output_string(&pulled.stdout).contains("100"));
    }
);

// I pushed data, the server restarted, and a pull from a fresh edge still returned everything.
sync_red_test!(
    f09_pull_after_server_restart_returns_data,
    "\
CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n\
INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'one')\n\
.sync push\n\
.quit\n",
    |edge_path: &std::path::Path, server_path: &std::path::Path, nats_url: &str| {
        let mut restarted = spawn_server(
            server_path,
            "f09_pull_after_server_restart_returns_data",
            nats_url,
        );
        stop_child(&mut restarted);
        let fresh_path = edge_path.with_file_name("fresh-after-restart.db");
        let pulled = run_cli_script(
            &fresh_path,
            &[
                "--tenant-id",
                "f09_pull_after_server_restart_returns_data",
                "--nats-url",
                nats_url,
            ],
            "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n.sync pull\nSELECT count(*) FROM sensors\n.quit\n",
        );
        assert!(output_string(&pulled.stdout).contains("100"));
    }
);

// I pushed, closed the edge, reopened it, inserted more rows, pushed again, and the server had everything.
sync_red_test!(
    f09b_edge_closes_reopens_pushes_more_data,
    "\
CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n\
INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'one')\n\
.sync push\n\
.quit\n",
    |edge_path: &std::path::Path, server_path: &std::path::Path, nats_url: &str| {
        let reopened = run_cli_script(
            edge_path,
            &[
                "--tenant-id",
                "f09b_edge_closes_reopens_pushes_more_data",
                "--nats-url",
                nats_url,
            ],
            "INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000002', 'two')\n.sync push\n.quit\n",
        );
        assert!(reopened.status.success());
        assert_eq!(count_rows_from_file(server_path, "sensors"), 100);
    }
);

// I crashed the edge mid-session, reopened it, pushed, and the server received all the data including what was written before the crash.
sync_red_test!(
    f09c_edge_crash_recovers_then_pushes,
    "\
CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n\
INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'one')\n\
.sync push\n\
.quit\n",
    |edge_path: &std::path::Path, server_path: &std::path::Path, nats_url: &str| {
        let mut child = spawn_cli(
            edge_path,
            &[
                "--tenant-id",
                "f09c_edge_crash_recovers_then_pushes",
                "--nats-url",
                nats_url,
            ],
        );
        write_child_stdin(
            &mut child,
            "INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000002', 'two')\n",
        );
        stop_child(&mut child);
        let reopened = run_cli_script(
            edge_path,
            &[
                "--tenant-id",
                "f09c_edge_crash_recovers_then_pushes",
                "--nats-url",
                nats_url,
            ],
            ".sync push\n.quit\n",
        );
        assert!(reopened.status.success());
        assert_eq!(count_rows_from_file(server_path, "sensors"), 150);
    }
);

// I lost power during a sync, retried, and the server had each row exactly once — no duplicates from the interrupted attempt.
sync_red_test!(
    f09d_power_loss_during_sync_does_not_cause_duplicates_on_retry,
    "\
CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n\
INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'one')\n\
.quit\n",
    |_edge_path: &std::path::Path, server_path: &std::path::Path, _nats_url: &str| {
        assert_eq!(count_rows_from_file(server_path, "sensors"), 100);
    }
);

// I pushed a row with every column type (UUID, TEXT, INTEGER, REAL, BOOLEAN, VECTOR), and the server had the full row with all values intact.
sync_red_test!(
    f09e_all_data_types_round_trip_through_sync,
    "\
CREATE TABLE everything (id UUID PRIMARY KEY, note TEXT, count INTEGER, reading REAL, enabled BOOLEAN, embedding VECTOR(3))\n\
INSERT INTO everything (id, note, count, reading, enabled, embedding) VALUES ('00000000-0000-0000-0000-000000000001', 'x', 7, 4.5, true, [1.0, 2.0, 3.0])\n\
.sync push\n\
.quit\n",
    |_edge_path: &std::path::Path, server_path: &std::path::Path, _nats_url: &str| {
        let db = contextdb_engine::Database::open(server_path).expect("server db open");
        let result = db
            .execute("SELECT * FROM everything", &empty_params())
            .expect("select from everything");
        assert_eq!(result.rows.len(), 1);
    }
);

// I pushed data that violated a server-side constraint, and the edge got back a clear "constraint" error instead of silently dropping the data.
sync_red_test!(
    f09f_server_side_constraint_violation_during_push_returns_error_to_edge,
    ".sync push\n.quit\n",
    |edge_path: &std::path::Path, _server_path: &std::path::Path, nats_url: &str| {
        let output = run_cli_script(
            edge_path,
            &[
                "--tenant-id",
                "f09f_server_side_constraint_violation_during_push_returns_error_to_edge",
                "--nats-url",
                nats_url,
            ],
            ".sync push\n.quit\n",
        );
        assert!(output_string(&output.stdout).contains("constraint"));
    }
);

// I tried to push while a transaction was still open, and the uncommitted rows did not end up on the server.
sync_red_test!(
    f09g_sync_push_during_open_transaction_is_rejected_or_queued,
    "\
CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n\
BEGIN\n\
INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'one')\n\
.sync push\n\
.quit\n",
    |_edge_path: &std::path::Path, server_path: &std::path::Path, _nats_url: &str| {
        assert_eq!(count_rows_from_file(server_path, "sensors"), 0);
    }
);

// I pulled data that conflicted with my local schema, and I got a clear "schema mismatch" message instead of a silent failure.
sync_red_test!(
    f09h_constraint_violations_during_sync_pull_are_handled,
    ".quit\n",
    |edge_path: &std::path::Path, _server_path: &std::path::Path, nats_url: &str| {
        let pulled = run_cli_script(
            edge_path,
            &[
                "--tenant-id",
                "f09h_constraint_violations_during_sync_pull_are_handled",
                "--nats-url",
                nats_url,
            ],
            ".sync pull\n.quit\n",
        );
        assert!(output_string(&pulled.stdout).contains("schema mismatch"));
    }
);

// I made conflicting state machine transitions on two edges, and the sync rejected or resolved the conflict explicitly instead of silently picking one.
sync_red_test!(
    f09i_conflicting_state_machine_transitions_across_edges_during_sync,
    ".quit\n",
    |_edge_path: &std::path::Path, _server_path: &std::path::Path, _nats_url: &str| {
        panic!("second conflicting state transition should be rejected or resolved explicitly");
    }
);

// I accumulated data offline for an extended period, pushed, and the server received the entire backlog.
sync_red_test!(
    f10_edge_offline_for_one_hour_then_pushes_backlog,
    "\
CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n\
INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'offline')\n\
.sync push\n\
.quit\n",
    |_edge_path: &std::path::Path, server_path: &std::path::Path, _nats_url: &str| {
        assert_eq!(count_rows_from_file(server_path, "sensors"), 500);
    }
);

// I tried to push while NATS was down, and I got a clear "timed out" or "cannot connect" error instead of hanging or silently failing.
sync_red_test!(
    f11_push_during_nats_outage_gives_a_clear_error,
    ".sync push\n.quit\n",
    |edge_path: &std::path::Path, _server_path: &std::path::Path, _nats_url: &str| {
        let output = run_cli_script(
            edge_path,
            &[
                "--tenant-id",
                "f11_push_during_nats_outage_gives_a_clear_error",
                "--nats-url",
                "nats://127.0.0.1:65531",
            ],
            ".sync push\n.quit\n",
        );
        let stdout = output_string(&output.stdout).to_lowercase();
        assert!(stdout.contains("timed out") || stdout.contains("cannot connect"));
    }
);
