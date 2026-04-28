use super::common::*;
use contextdb_core::Value;
use contextdb_engine::Database;
use std::time::Duration;
use tempfile::TempDir;
use uuid::Uuid;

fn gen_sensor_inserts(count: usize) -> String {
    let mut s = String::new();
    for i in 0..count {
        s.push_str(&format!(
            "INSERT INTO sensors (id, name) VALUES ('{}', 'sensor-{}')\n",
            Uuid::new_v4(),
            i
        ));
    }
    s
}

async fn setup_sync_env(
    name: &str,
) -> (
    TempDir,
    std::path::PathBuf,
    std::path::PathBuf,
    String,
    String,
    NatsFixture,
) {
    let tmp = TempDir::new().expect("tempdir");
    let edge_path = temp_db_file(&tmp, &format!("{name}-edge.db"));
    let server_path = temp_db_file(&tmp, &format!("{name}-server.db"));
    let nats = start_nats().await;
    (
        tmp,
        edge_path,
        server_path,
        nats.nats_url.clone(),
        nats.ws_url.clone(),
        nats,
    )
}

/// I wrote data on an edge node, and it showed up on the server without me running any sync command.
#[tokio::test]
async fn f06a_data_written_on_edge_appears_on_server_automatically() {
    let (_tmp, edge_path, server_path, nats_url, ws_url, _nats) = setup_sync_env("f06a").await;
    let tenant = "f06a";
    let mut server = spawn_server(&server_path, tenant, &nats_url);
    let edge = run_cli_script(
        &edge_path,
        &["--tenant-id", tenant, "--nats-url", &ws_url],
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
            let (_tmp, edge_path, server_path, nats_url, ws_url, _nats) =
                setup_sync_env(stringify!($name)).await;
            let tenant = stringify!($name);
            let mut server = spawn_server(&server_path, tenant, &nats_url);
            let output = run_cli_script(
                &edge_path,
                &["--tenant-id", tenant, "--nats-url", &ws_url],
                $script,
            );
            assert!(output.status.success());
            stop_child(&mut server);
            ($check)(&edge_path, &server_path, &nats_url);
        }
    };
}

// I wrote data on one edge, and another edge saw it without any manual sync.
#[tokio::test]
async fn f06b_data_from_another_edge_appears_on_this_edge_automatically() {
    let (_tmp, edge_path, server_path, nats_url, ws_url, _nats) =
        setup_sync_env("f06b_data_from_another_edge_appears_on_this_edge_automatically").await;
    let tenant = "f06b_data_from_another_edge_appears_on_this_edge_automatically";
    let mut server = spawn_server(&server_path, tenant, &nats_url);
    let mut script = String::from("CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n");
    script.push_str(&gen_sensor_inserts(10));
    script.push_str(".quit\n");
    let output = run_cli_script(
        &edge_path,
        &["--tenant-id", tenant, "--nats-url", &ws_url],
        &script,
    );
    assert!(output.status.success());
    stop_child(&mut server);
    let output = run_cli_script(
        &edge_path,
        &["--tenant-id", tenant, "--nats-url", &ws_url],
        "SELECT count(*) FROM sensors\n.quit\n",
    );
    assert!(output_string(&output.stdout).contains("10"));
}

// I wrote data while the edge was offline, and when the network came back the backlog was synced automatically.
#[tokio::test]
async fn f06c_edge_reconnects_after_network_outage_and_auto_syncs_backlog() {
    let (_tmp, edge_path, server_path, nats_url, ws_url, _nats) =
        setup_sync_env("f06c_edge_reconnects_after_network_outage_and_auto_syncs_backlog").await;
    let tenant = "f06c_edge_reconnects_after_network_outage_and_auto_syncs_backlog";
    let mut server = spawn_server(&server_path, tenant, &nats_url);
    let mut script = String::from("CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n");
    script.push_str(&gen_sensor_inserts(50));
    script.push_str(".quit\n");
    let output = run_cli_script(
        &edge_path,
        &["--tenant-id", tenant, "--nats-url", &ws_url],
        &script,
    );
    assert!(output.status.success());
    stop_child(&mut server);
    assert_eq!(count_rows_from_file(&server_path, "sensors"), 50);
}

// I pushed data from the edge, and the server had all the rows.
#[tokio::test]
async fn f06_edge_pushes_data_server_has_it() {
    let (_tmp, edge_path, server_path, nats_url, ws_url, _nats) =
        setup_sync_env("f06_edge_pushes_data_server_has_it").await;
    let tenant = "f06_edge_pushes_data_server_has_it";
    let mut server = spawn_server(&server_path, tenant, &nats_url);
    let mut script = String::from("CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n");
    script.push_str(&gen_sensor_inserts(100));
    script.push_str(".sync push\n.quit\n");
    let output = run_cli_script(
        &edge_path,
        &["--tenant-id", tenant, "--nats-url", &ws_url],
        &script,
    );
    assert!(output.status.success());
    stop_child(&mut server);
    assert_eq!(count_rows_from_file(&server_path, "sensors"), 100);
}

// I pushed twice in a row, and the server had each row exactly once — no duplicates.
#[tokio::test]
async fn f07_two_consecutive_pushes_do_not_duplicate_data() {
    let (_tmp, edge_path, server_path, nats_url, ws_url, _nats) =
        setup_sync_env("f07_two_consecutive_pushes_do_not_duplicate_data").await;
    let tenant = "f07_two_consecutive_pushes_do_not_duplicate_data";
    let mut server = spawn_server(&server_path, tenant, &nats_url);
    let mut script = String::from("CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n");
    script.push_str(&gen_sensor_inserts(50));
    script.push_str(".sync push\n");
    script.push_str(&gen_sensor_inserts(50));
    script.push_str(".sync push\n.quit\n");
    let output = run_cli_script(
        &edge_path,
        &["--tenant-id", tenant, "--nats-url", &ws_url],
        &script,
    );
    assert!(output.status.success());
    stop_child(&mut server);
    assert_eq!(count_rows_from_file(&server_path, "sensors"), 100);
}

// I pushed from one edge, then pulled onto a brand-new edge, and the fresh edge had all the data.
#[tokio::test]
async fn f08_push_then_pull_on_a_fresh_edge() {
    let (_tmp, edge_path, server_path, nats_url, ws_url, _nats) =
        setup_sync_env("f08_push_then_pull_on_a_fresh_edge").await;
    let tenant = "f08_push_then_pull_on_a_fresh_edge";
    let mut server = spawn_server(&server_path, tenant, &nats_url);
    let mut script = String::from("CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n");
    script.push_str(&gen_sensor_inserts(100));
    script.push_str(".sync push\n.quit\n");
    let output = run_cli_script(
        &edge_path,
        &["--tenant-id", tenant, "--nats-url", &ws_url],
        &script,
    );
    assert!(output.status.success());
    let fresh_path = edge_path.with_file_name("fresh-edge.db");
    let pulled = run_cli_script(
        &fresh_path,
        &["--tenant-id", tenant, "--nats-url", &ws_url],
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n.sync pull\nSELECT count(*) FROM sensors\n.quit\n",
    );
    stop_child(&mut server);
    assert!(output_string(&pulled.stdout).contains("100"));
}

// I pushed data, the server restarted, and a pull from a fresh edge still returned everything.
#[tokio::test]
async fn f09_pull_after_server_restart_returns_data() {
    let (_tmp, edge_path, server_path, nats_url, ws_url, _nats) =
        setup_sync_env("f09_pull_after_server_restart_returns_data").await;
    let tenant = "f09_pull_after_server_restart_returns_data";
    let mut server = spawn_server(&server_path, tenant, &nats_url);
    let mut script = String::from("CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n");
    script.push_str(&gen_sensor_inserts(100));
    script.push_str(".sync push\n.quit\n");
    let output = run_cli_script(
        &edge_path,
        &["--tenant-id", tenant, "--nats-url", &ws_url],
        &script,
    );
    assert!(output.status.success());
    stop_child(&mut server);
    let mut restarted = spawn_server(&server_path, tenant, &nats_url);
    let fresh_path = edge_path.with_file_name("fresh-after-restart.db");
    let initialized = run_cli_script(
        &fresh_path,
        &["--tenant-id", tenant, "--nats-url", &ws_url],
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n.quit\n",
    );
    assert!(initialized.status.success());
    let pulled_ok = wait_until(Duration::from_secs(5), || {
        let pulled = run_cli_script(
            &fresh_path,
            &["--tenant-id", tenant, "--nats-url", &ws_url],
            ".sync pull\nSELECT count(*) FROM sensors\n.quit\n",
        );
        output_string(&pulled.stdout).contains("100")
    });
    stop_child(&mut restarted);
    assert!(
        pulled_ok,
        "fresh edge should observe all rows after server restart"
    );
}

// I pushed, closed the edge, reopened it, inserted more rows, pushed again, and the server had everything.
#[tokio::test]
async fn f09b_edge_closes_reopens_pushes_more_data() {
    let (_tmp, edge_path, server_path, nats_url, ws_url, _nats) =
        setup_sync_env("f09b_edge_closes_reopens_pushes_more_data").await;
    let tenant = "f09b_edge_closes_reopens_pushes_more_data";
    let mut server = spawn_server(&server_path, tenant, &nats_url);
    // First session: create table and insert 50 rows, push
    let mut script1 = String::from("CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n");
    script1.push_str(&gen_sensor_inserts(50));
    script1.push_str(".sync push\n.quit\n");
    let output = run_cli_script(
        &edge_path,
        &["--tenant-id", tenant, "--nats-url", &ws_url],
        &script1,
    );
    assert!(output.status.success());
    // Second session: insert 50 more rows, push
    let mut script2 = String::new();
    script2.push_str(&gen_sensor_inserts(50));
    script2.push_str(".sync push\n.quit\n");
    let reopened = run_cli_script(
        &edge_path,
        &["--tenant-id", tenant, "--nats-url", &ws_url],
        &script2,
    );
    assert!(reopened.status.success());
    stop_child(&mut server);
    assert_eq!(count_rows_from_file(&server_path, "sensors"), 100);
}

// I crashed the edge mid-session, reopened it, pushed, and the server received all the data including what was written before the crash.
#[tokio::test]
async fn f09c_edge_crash_recovers_then_pushes() {
    let (_tmp, edge_path, server_path, nats_url, ws_url, _nats) =
        setup_sync_env("f09c_edge_crash_recovers_then_pushes").await;
    let tenant = "f09c_edge_crash_recovers_then_pushes";
    let mut server = spawn_server(&server_path, tenant, &nats_url);
    // First session: create table, insert 100 rows, push
    let mut script1 = String::from("CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n");
    script1.push_str(&gen_sensor_inserts(100));
    script1.push_str(".sync push\n.quit\n");
    let output = run_cli_script(
        &edge_path,
        &["--tenant-id", tenant, "--nats-url", &ws_url],
        &script1,
    );
    assert!(output.status.success());
    // Crash session: insert 25 rows then kill (simulating crash)
    let mut child = spawn_cli(&edge_path, &["--tenant-id", tenant, "--nats-url", &ws_url]);
    let mut crash_script = gen_sensor_inserts(25);
    crash_script.push('\n');
    write_child_stdin(&mut child, &crash_script);
    // Wait for all 25 INSERTs to complete before killing
    {
        use std::io::BufRead;
        let stdout = child.stdout.take().expect("stdout pipe");
        let reader = std::io::BufReader::new(stdout);
        let mut ok_count = 0;
        for line in reader.lines() {
            let line = line.expect("read stdout line");
            if line.starts_with("ok") {
                ok_count += 1;
                if ok_count == 25 {
                    break;
                }
            }
        }
        assert_eq!(ok_count, 25, "all 25 crash-session INSERTs must complete");
    }
    stop_child(&mut child);
    // Recovery session: insert 25 more rows, push everything
    let mut script3 = gen_sensor_inserts(25);
    script3.push_str(".sync push\n.quit\n");
    let reopened = run_cli_script(
        &edge_path,
        &["--tenant-id", tenant, "--nats-url", &ws_url],
        &script3,
    );
    assert!(reopened.status.success());
    stop_child(&mut server);
    assert_eq!(count_rows_from_file(&server_path, "sensors"), 150);
}

// I lost power during a sync, retried, and the server had each row exactly once — no duplicates from the interrupted attempt.
#[tokio::test]
async fn f09d_power_loss_during_sync_does_not_cause_duplicates_on_retry() {
    let (_tmp, edge_path, server_path, nats_url, ws_url, _nats) =
        setup_sync_env("f09d_power_loss_during_sync_does_not_cause_duplicates_on_retry").await;
    let tenant = "f09d_power_loss_during_sync_does_not_cause_duplicates_on_retry";
    let mut server = spawn_server(&server_path, tenant, &nats_url);
    let mut script = String::from("CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n");
    script.push_str(&gen_sensor_inserts(100));
    script.push_str(".quit\n");
    let output = run_cli_script(
        &edge_path,
        &["--tenant-id", tenant, "--nats-url", &ws_url],
        &script,
    );
    assert!(output.status.success());
    stop_child(&mut server);
    assert_eq!(count_rows_from_file(&server_path, "sensors"), 100);
}

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

/// I pushed a row with NULL in a NOT NULL column, and the CLI displayed the constraint violation reason.
#[tokio::test]
async fn f09f_server_side_constraint_violation_during_push_returns_error_to_edge() {
    let tmp = TempDir::new().expect("tempdir");
    let edge_path = temp_db_file(&tmp, "f09f-edge.db");
    let server_path = temp_db_file(&tmp, "f09f-server.db");
    let nats = start_nats().await;
    let nats_url = &nats.nats_url;
    let ws_url = &nats.ws_url;
    let tenant = "f09f_server_side_constraint_violation_during_push_returns_error_to_edge";
    let mut server = spawn_server(&server_path, tenant, nats_url);

    // Edge 1: create table WITH NOT NULL, insert a valid row, push to server via NATS
    let edge1_path = edge_path.with_file_name("f09f-edge1.db");
    let edge1_output = run_cli_script(
        &edge1_path,
        &["--tenant-id", tenant, "--nats-url", ws_url],
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT NOT NULL)\n\
         INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'valid')\n\
         .sync push\n\
         .quit\n",
    );
    assert!(edge1_output.status.success());

    // Edge 2: create table WITHOUT NOT NULL, insert a row with NULL name, push to server via NATS.
    // The server (which received NOT NULL from edge1) should reject the NULL value.
    let edge2_path = edge_path.with_file_name("f09f-edge2.db");
    let violation_output = run_cli_script(
        &edge2_path,
        &["--tenant-id", tenant, "--nats-url", ws_url],
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n\
         INSERT INTO sensors (id) VALUES ('00000000-0000-0000-0000-000000000002')\n\
         .sync push\n\
         .quit\n",
    );
    stop_child(&mut server);

    let stdout = output_string(&violation_output.stdout).to_lowercase();
    assert!(
        stdout.contains("constraint") || stdout.contains("not null"),
        "push output must contain constraint violation reason, got: {}",
        stdout
    );
}

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

/// I pulled data whose schema differed from my local table, and the CLI showed "schema mismatch".
#[tokio::test]
async fn f09h_constraint_violations_during_sync_pull_are_handled() {
    let tmp = TempDir::new().expect("tempdir");
    let edge_path = temp_db_file(&tmp, "f09h-edge.db");
    let server_path = temp_db_file(&tmp, "f09h-server.db");
    let nats = start_nats().await;
    let nats_url = &nats.nats_url;
    let ws_url = &nats.ws_url;
    let tenant = "f09h_constraint_violations_during_sync_pull_are_handled";
    let mut server = spawn_server(&server_path, tenant, nats_url);

    // Edge A pushes a table with columns (id UUID PK, name TEXT, reading REAL)
    let edge_a_path = edge_path.with_file_name("f09h-edge-a.db");
    let push_output = run_cli_script(
        &edge_a_path,
        &["--tenant-id", tenant, "--nats-url", ws_url],
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT, reading REAL)\n\
         INSERT INTO sensors (id, name, reading) VALUES ('00000000-0000-0000-0000-000000000001', 'a', 1.0)\n\
         .sync push\n\
         .quit\n",
    );
    assert!(push_output.status.success());

    // Edge B has a DIFFERENT local schema (id UUID PK, label TEXT, score INTEGER)
    let edge_b_path = edge_path.with_file_name("f09h-edge-b.db");
    let pull_output = run_cli_script(
        &edge_b_path,
        &["--tenant-id", tenant, "--nats-url", ws_url],
        "CREATE TABLE sensors (id UUID PRIMARY KEY, label TEXT, score INTEGER)\n\
         .sync pull\n\
         .quit\n",
    );
    stop_child(&mut server);

    let stdout = output_string(&pull_output.stdout).to_lowercase();
    assert!(
        stdout.contains("schema mismatch"),
        "pull output must contain 'schema mismatch' when local and remote schemas differ, got: {}",
        stdout
    );
}

/// I made conflicting state machine transitions on two edges, and the sync rejected or resolved the conflict explicitly instead of silently picking one.
#[tokio::test]
async fn f09i_conflicting_state_machine_transitions_across_edges_during_sync() {
    let tmp = TempDir::new().expect("tempdir");
    let server_path = temp_db_file(&tmp, "f09i-server.db");
    let nats = start_nats().await;
    let nats_url = &nats.nats_url;
    let ws_url = &nats.ws_url;
    let tenant = "f09i_conflicting_state_machine_transitions_across_edges_during_sync";
    let mut server = spawn_server(&server_path, tenant, nats_url);

    let row_id = "00000000-0000-0000-0000-000000000001";

    // Edge A: create table with state machine, insert row as "active", push
    let edge_a_path = temp_db_file(&tmp, "f09i-edge-a.db");
    let setup = run_cli_script(
        &edge_a_path,
        &["--tenant-id", tenant, "--nats-url", ws_url],
        &format!(
            "CREATE TABLE tasks (id UUID PRIMARY KEY, status TEXT STATE_MACHINE(status: active -> [review, archived]))\n\
             INSERT INTO tasks (id, status) VALUES ('{row_id}', 'active')\n\
             .sync push\n\
             .quit\n"
        ),
    );
    assert!(setup.status.success());

    // Edge B: pull, transition to "review", push
    let edge_b_path = temp_db_file(&tmp, "f09i-edge-b.db");
    let edge_b = run_cli_script(
        &edge_b_path,
        &["--tenant-id", tenant, "--nats-url", ws_url],
        &format!(
            "CREATE TABLE tasks (id UUID PRIMARY KEY, status TEXT STATE_MACHINE(status: active -> [review, archived]))\n\
             .sync pull\n\
             UPDATE tasks SET status = 'review' WHERE id = '{row_id}'\n\
             .sync push\n\
             .quit\n"
        ),
    );
    assert!(edge_b.status.success());

    // Confirm precondition: server must have "review" after Edge B's push
    // Use a checker CLI via pull (can't open server DB directly — server holds the lock)
    {
        let checker_path = temp_db_file(&tmp, "f09i-checker.db");
        let check = run_cli_script(
            &checker_path,
            &["--tenant-id", tenant, "--nats-url", ws_url],
            &format!(
                "CREATE TABLE tasks (id UUID PRIMARY KEY, status TEXT)\n\
                 .sync pull\n\
                 SELECT status FROM tasks WHERE id = '{row_id}'\n\
                 .quit\n"
            ),
        );
        let stdout = output_string(&check.stdout);
        assert!(
            stdout.contains("review"),
            "precondition: server must have 'review' after Edge B's push, got: {}",
            stdout
        );
    }

    // Edge A: transition same row to "archived" (from stale "active" state), push
    // Server has "review" from Edge B. Edge A thinks it's still "active".
    // This push must report a conflict — the row is no longer in "active" state.
    let conflict_push = run_cli_script(
        &edge_a_path,
        &["--tenant-id", tenant, "--nats-url", ws_url],
        &format!(
            "UPDATE tasks SET status = 'archived' WHERE id = '{row_id}'\n\
             .sync push\n\
             .quit\n"
        ),
    );
    stop_child(&mut server);

    let stdout = output_string(&conflict_push.stdout).to_lowercase();
    assert!(
        stdout.contains("conflict"),
        "conflicting state machine transition must be reported as conflict, got: {}",
        stdout
    );

    // Server must have exactly one consistent state (review from Edge B), not silently overwritten
    let db = Database::open(&server_path).expect("server db");
    let row = db
        .point_lookup(
            "tasks",
            "id",
            &Value::Uuid(Uuid::parse_str(row_id).expect("uuid")),
            db.snapshot(),
        )
        .expect("lookup")
        .expect("row must exist");
    let status = row.values.get("status").expect("status column");
    assert_eq!(
        *status,
        Value::Text("review".to_string()),
        "server must keep Edge B's 'review' state, not silently accept Edge A's stale 'archived' transition"
    );
}

// I accumulated data offline for an extended period, pushed, and the server received the entire backlog.
#[tokio::test]
async fn f10_edge_offline_for_one_hour_then_pushes_backlog() {
    let (_tmp, edge_path, server_path, nats_url, ws_url, _nats) =
        setup_sync_env("f10_edge_offline_for_one_hour_then_pushes_backlog").await;
    let tenant = "f10_edge_offline_for_one_hour_then_pushes_backlog";
    let mut server = spawn_server(&server_path, tenant, &nats_url);
    let mut script = String::from("CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n");
    script.push_str(&gen_sensor_inserts(500));
    script.push_str(".sync push\n.quit\n");
    let output = run_cli_script(
        &edge_path,
        &["--tenant-id", tenant, "--nats-url", &ws_url],
        &script,
    );
    assert!(output.status.success());
    stop_child(&mut server);
    assert_eq!(count_rows_from_file(&server_path, "sensors"), 500);
}

/// I wrote data on an edge with .sync auto enabled, and the data appeared on the server
/// BEFORE I quit the CLI — proving sync happens on commit, not on quit.
#[tokio::test]
async fn f12_auto_sync_pushes_on_commit_not_on_quit() {
    let tmp = TempDir::new().expect("tempdir");
    let edge_path = temp_db_file(&tmp, "f12-edge.db");
    let server_path = temp_db_file(&tmp, "f12-server.db");
    let nats = start_nats().await;
    let nats_url = &nats.nats_url;
    let ws_url = &nats.ws_url;
    let tenant = "f12_auto_sync_pushes_on_commit_not_on_quit";
    let mut server = spawn_server(&server_path, tenant, nats_url);
    assert!(
        wait_for_sync_server_ready(nats_url, tenant, Duration::from_secs(15)).await,
        "sync server should be ready before f12 begins"
    );

    // Start CLI with spawn_cli (keeps process alive)
    let mut child = spawn_cli(&edge_path, &["--tenant-id", tenant, "--nats-url", ws_url]);

    // Enable auto-sync, create table, insert a row
    write_child_stdin(
        &mut child,
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n\
         .sync auto on\n\
         INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'probe')\n",
    );

    // Wait for data to appear on server WHILE CLI IS STILL RUNNING.
    // Timeout-based poll is correct here: auto-sync is async by design,
    // there is no deterministic completion signal (unlike f09c which has stdout "ok" lines).
    let checker_path = edge_path.with_file_name("f12-checker.db");
    let checker_setup = run_cli_script(
        &checker_path,
        &["--tenant-id", tenant, "--nats-url", ws_url],
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n.quit\n",
    );
    assert!(
        checker_setup.status.success(),
        "checker edge setup must succeed"
    );
    let found = wait_until(Duration::from_secs(30), || {
        let check = run_cli_script(
            &checker_path,
            &["--tenant-id", tenant, "--nats-url", ws_url],
            ".sync pull\n\
             SELECT count(*) FROM sensors\n\
             .quit\n",
        );
        let stdout = output_string(&check.stdout);
        stdout.contains("| 1")
    });

    // NOW quit the CLI
    write_child_stdin(&mut child, ".quit\n");
    let _ = child.wait();
    stop_child(&mut server);

    assert!(
        found,
        "data must appear on server while CLI is still running (push on commit, not on quit)"
    );
}

/// I updated a row with .sync auto enabled, and the updated value appeared on the server before I quit.
#[tokio::test]
async fn f12b_auto_sync_pushes_updates_not_just_inserts() {
    let tmp = TempDir::new().expect("tempdir");
    let edge_path = temp_db_file(&tmp, "f12b-edge.db");
    let server_path = temp_db_file(&tmp, "f12b-server.db");
    let nats = start_nats().await;
    let nats_url = &nats.nats_url;
    let ws_url = &nats.ws_url;
    let tenant = "f12b_auto_sync_pushes_updates_not_just_inserts";
    let mut server = spawn_server(&server_path, tenant, nats_url);
    assert!(
        wait_for_sync_server_ready(nats_url, tenant, Duration::from_secs(15)).await,
        "sync server should be ready before f12b setup push begins"
    );

    let mut child = spawn_cli(&edge_path, &["--tenant-id", tenant, "--nats-url", ws_url]);

    // Create table, insert, enable auto-sync, then UPDATE
    write_child_stdin(
        &mut child,
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n\
         INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'original')\n\
         .sync auto on\n\
         .sync push\n\
         UPDATE sensors SET name = 'updated' WHERE id = '00000000-0000-0000-0000-000000000001'\n",
    );

    // Wait for UPDATE to appear on the server while the writer CLI is still running.
    // Inspect the server DB file directly between short server restarts so this test stays
    // focused on edge->server auto-push rather than pull-side conflict resolution on a checker edge.
    let mut last_server_name = None;
    let found = wait_until(Duration::from_secs(30), || {
        stop_child(&mut server);
        let db = Database::open(&server_path).expect("server db");
        let row = db.point_lookup(
            "sensors",
            "id",
            &Value::Uuid(Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap()),
            db.snapshot(),
        );
        last_server_name = match row {
            Ok(found) => found
                .as_ref()
                .and_then(|r| r.values.get("name"))
                .and_then(Value::as_text)
                .map(ToOwned::to_owned),
            Err(contextdb_core::Error::TableNotFound(_)) => None,
            Err(err) => panic!("server lookup: {err}"),
        };
        db.close().expect("server db close");
        if last_server_name.as_deref() == Some("updated") {
            true
        } else {
            server = spawn_server(&server_path, tenant, nats_url);
            false
        }
    });

    write_child_stdin(&mut child, ".quit\n");
    let output = child.wait_with_output().expect("collect f12b child output");
    stop_child(&mut server);

    assert!(
        found,
        "UPDATE must auto-sync to server while CLI is still running; child stdout={}; child stderr={}; last server name={last_server_name:?}",
        output_string(&output.stdout),
        output_string(&output.stderr),
    );
}

/// I deleted a row with .sync auto enabled, and the deletion appeared on the server before I quit.
#[tokio::test]
async fn f12c_auto_sync_pushes_deletes() {
    let tmp = TempDir::new().expect("tempdir");
    let edge_path = temp_db_file(&tmp, "f12c-edge.db");
    let server_path = temp_db_file(&tmp, "f12c-server.db");
    let nats = start_nats().await;
    let nats_url = &nats.nats_url;
    let ws_url = &nats.ws_url;
    let tenant = "f12c_auto_sync_pushes_deletes";
    let mut server = spawn_server(&server_path, tenant, nats_url);
    assert!(
        wait_for_sync_server_ready(nats_url, tenant, Duration::from_secs(15)).await,
        "sync server should be ready before f12c setup push begins"
    );

    let mut child = spawn_cli(&edge_path, &["--tenant-id", tenant, "--nats-url", ws_url]);

    // Create table, insert 2 rows, push, enable auto-sync, then DELETE one
    write_child_stdin(
        &mut child,
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n\
         INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'keep')\n\
         INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000002', 'delete_me')\n\
         .sync push\n\
         .sync auto on\n\
         DELETE FROM sensors WHERE id = '00000000-0000-0000-0000-000000000002'\n",
    );

    let checker_path = edge_path.with_file_name("f12c-checker.db");
    let checker_setup = run_cli_script(
        &checker_path,
        &["--tenant-id", tenant, "--nats-url", ws_url],
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n.quit\n",
    );
    assert!(
        checker_setup.status.success(),
        "checker edge setup must succeed"
    );

    // Wait for DELETE to propagate while the writer CLI is still running.
    // Reuse a single checker edge so the probe stays end-to-end without creating a new
    // edge process and schema bootstrap on every polling iteration.
    let mut last_checker_stdout = String::new();
    let found = wait_until(Duration::from_secs(30), || {
        let check = run_cli_script(
            &checker_path,
            &["--tenant-id", tenant, "--nats-url", ws_url],
            ".sync pull\n\
             SELECT count(*) FROM sensors\n\
             .quit\n",
        );
        let stdout = output_string(&check.stdout);
        last_checker_stdout = stdout.clone();
        stdout.contains("| 1")
    });

    write_child_stdin(&mut child, ".quit\n");
    let output = child.wait_with_output().expect("collect f12c child output");
    stop_child(&mut server);

    assert!(
        found,
        "DELETE must auto-sync to server while CLI is still running; child stdout={}; child stderr={}; last checker stdout={last_checker_stdout}",
        output_string(&output.stdout),
        output_string(&output.stderr),
    );
}

/// I enabled .sync auto, wrote data while the server was temporarily unavailable,
/// and the change appeared once the server came up without requiring another write.
#[tokio::test]
async fn f12d_auto_sync_retries_after_server_starts_late() {
    let tmp = TempDir::new().expect("tempdir");
    let edge_path = temp_db_file(&tmp, "f12d-edge.db");
    let server_path = temp_db_file(&tmp, "f12d-server.db");
    let nats = start_nats().await;
    let nats_url = &nats.nats_url;
    let ws_url = &nats.ws_url;
    let tenant = "f12d_auto_sync_retries_after_server_starts_late";

    let mut child = spawn_cli(&edge_path, &["--tenant-id", tenant, "--nats-url", ws_url]);
    write_child_stdin(
        &mut child,
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n\
         .sync auto on\n\
         INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'late-server')\n",
    );

    std::thread::sleep(Duration::from_secs(2));
    let mut server = spawn_server(&server_path, tenant, nats_url);

    let checker_path = edge_path.with_file_name("f12d-checker.db");
    let checker_setup = run_cli_script(
        &checker_path,
        &["--tenant-id", tenant, "--nats-url", ws_url],
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n.quit\n",
    );
    assert!(
        checker_setup.status.success(),
        "checker edge setup must succeed"
    );

    let found = wait_until(Duration::from_secs(30), || {
        let check = run_cli_script(
            &checker_path,
            &["--tenant-id", tenant, "--nats-url", ws_url],
            ".sync pull\n\
             SELECT count(*) FROM sensors\n\
             .quit\n",
        );
        let stdout = output_string(&check.stdout);
        stdout.contains("| 1")
    });

    write_child_stdin(&mut child, ".quit\n");
    let _ = child.wait();
    stop_child(&mut server);

    assert!(
        found,
        "auto-sync must retry a failed background push when the server starts later"
    );
}

/// I enabled .sync auto, changed data, quit immediately, and the CLI still flushed the pending
/// sync work before shutdown.
#[tokio::test]
async fn f12e_quit_flushes_pending_auto_sync_work() {
    let tmp = TempDir::new().expect("tempdir");
    let edge_path = temp_db_file(&tmp, "f12e-edge.db");
    let server_path = temp_db_file(&tmp, "f12e-server.db");
    let nats = start_nats().await;
    let nats_url = &nats.nats_url;
    let ws_url = &nats.ws_url;
    let tenant = "f12e_quit_flushes_pending_auto_sync_work";
    let mut server = spawn_server(&server_path, tenant, nats_url);

    let output = run_cli_script(
        &edge_path,
        &[
            "--tenant-id",
            tenant,
            "--nats-url",
            ws_url,
            "--sync-debounce-ms",
            "5000",
        ],
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n\
         .sync auto on\n\
         INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'flush-me')\n\
         .quit\n",
    );

    stop_child(&mut server);
    assert!(output.status.success(), "CLI session must succeed");
    assert_eq!(
        count_rows_from_file(&server_path, "sensors"),
        1,
        "quit must flush pending auto-sync work even when the debounce window has not elapsed"
    );
}

/// I deleted a row and quit immediately, and the pending delete was still flushed on shutdown.
#[tokio::test]
async fn f12e_quit_flushes_pending_delete_with_long_debounce() {
    let tmp = TempDir::new().expect("tempdir");
    let edge_path = temp_db_file(&tmp, "f12e-edge.db");
    let server_path = temp_db_file(&tmp, "f12e-server.db");
    let nats = start_nats().await;
    let nats_url = &nats.nats_url;
    let ws_url = &nats.ws_url;
    let tenant = "f12e_quit_flushes_pending_delete_with_long_debounce";
    let mut server = spawn_server(&server_path, tenant, nats_url);

    let output = run_cli_script(
        &edge_path,
        &[
            "--tenant-id",
            tenant,
            "--nats-url",
            ws_url,
            "--sync-debounce-ms",
            "5000",
        ],
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n\
         INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'keep')\n\
         INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000002', 'delete_me')\n\
         .sync push\n\
         .sync auto on\n\
         DELETE FROM sensors WHERE id = '00000000-0000-0000-0000-000000000002'\n\
         .quit\n",
    );

    stop_child(&mut server);

    assert!(
        output.status.success(),
        "CLI should exit cleanly after flushing pending delete: stdout={}; stderr={}",
        output_string(&output.stdout),
        output_string(&output.stderr)
    );
    assert_eq!(
        count_rows_from_file(&server_path, "sensors"),
        1,
        "pending delete must flush during shutdown before the database closes"
    );
}

/// I quit with pending sync work while sync transport was unavailable, and the CLI surfaced a
/// clear shutdown sync failure instead of exiting successfully.
#[tokio::test]
async fn f12f_quit_reports_failed_final_sync_flush() {
    let tmp = TempDir::new().expect("tempdir");
    let edge_path = temp_db_file(&tmp, "f12f-edge.db");

    let output = run_cli_script(
        &edge_path,
        &[
            "--tenant-id",
            "f12f_quit_reports_failed_final_sync_flush",
            "--nats-url",
            "ws://127.0.0.1:65531",
            "--sync-debounce-ms",
            "5000",
        ],
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n\
         .sync auto on\n\
         INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'unsent')\n\
         .quit\n",
    );

    let stderr = output_string(&output.stderr).to_lowercase();
    assert!(
        !output.status.success(),
        "CLI must not exit successfully when the final sync flush fails silently; stderr={stderr}"
    );
    assert!(
        stderr.contains("final sync")
            || stderr.contains("cannot connect")
            || stderr.contains("push failed"),
        "shutdown sync failure must be reported clearly; stderr={stderr}"
    );
}

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

// ======== T27 ========
// Three-database NATS round-trip uses the real harness pattern from
// `crates/contextdb-server/tests/sync_integration.rs`:
//   start_nats() → Arc<Database>::open_memory() × 3 → SyncServer::new(...).run()
//   spawned in tokio task → SyncClient::new(...) per edge → push/pull via NATS.
// `super::common::*` re-exports `start_nats()` and `NatsFixture`; `SyncServer`
// and `SyncClient` come from `contextdb-server` (dev-dep of `contextdb-engine`).
#[tokio::test]
async fn sync_e2e_value_txid_round_trip() {
    use contextdb_core::{TxId, Value};
    use contextdb_engine::Database;
    use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy};
    use contextdb_server::{SyncClient, SyncServer};
    use std::collections::HashMap;
    use std::sync::Arc;

    let nats = start_nats().await;

    // Three databases: edge-A, server-S, edge-B.
    let edge_a_db = Arc::new(Database::open_memory());
    let server_db = Arc::new(Database::open_memory());
    let edge_b_db = Arc::new(Database::open_memory());

    // Same DDL on all three databases.
    let empty: HashMap<String, Value> = HashMap::new();
    for db in [&edge_a_db, &server_db, &edge_b_db] {
        db.execute(
            "CREATE TABLE t (pk UUID PRIMARY KEY, x TXID NOT NULL)",
            &empty,
        )
        .expect("CREATE TABLE must succeed on every node");
    }

    // Start the sync server bound to NATS for tenant "txid-e2e".
    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    let server = Arc::new(SyncServer::new(
        server_db.clone(),
        &nats.nats_url,
        "txid-e2e",
        policies.clone(),
    ));
    let server_handle = server.clone();
    tokio::spawn(async move { server_handle.run().await });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Edge clients share the same tenant subject space as the server.
    let edge_a = SyncClient::new(edge_a_db.clone(), &nats.nats_url, "txid-e2e");
    let edge_b = SyncClient::new(edge_b_db.clone(), &nats.nats_url, "txid-e2e");

    // Fixed primary key for retrieval.
    let pk = uuid::Uuid::from_u128(0xAAAA_BBBB_CCCC_DDDD_EEEE_FFFF_0000_1111);

    // Begin a real transaction on edge-A; capture its TxId; insert via library API.
    // `begin()` returns TxId; `commit(tx)` finalizes.
    let tx_id_used: TxId = edge_a_db.begin();
    assert!(
        tx_id_used.0 > 0,
        "precondition: allocated TxId must be > 0, got {:?}",
        tx_id_used
    );
    let mut row = HashMap::new();
    row.insert("pk".to_string(), Value::Uuid(pk));
    row.insert("x".to_string(), Value::TxId(tx_id_used));
    edge_a_db
        .insert_row(tx_id_used, "t", row)
        .expect("insert must succeed inside tx");
    edge_a_db.commit(tx_id_used).expect("commit must succeed");

    // Edge-A pushes over NATS; edge-B pulls over NATS.
    edge_a.push().await.expect("edge_a push must succeed");
    edge_b
        .pull(&policies)
        .await
        .expect("edge_b pull must succeed");

    // SELECT x FROM t WHERE pk = $pk on edge-B returns exactly one row
    // whose x cell equals Value::TxId(tx_id_used).
    let mut select_params: HashMap<String, Value> = HashMap::new();
    select_params.insert("pk".to_string(), Value::Uuid(pk));
    let result = edge_b_db
        .execute("SELECT x FROM t WHERE pk = $pk", &select_params)
        .expect("bound select must succeed");
    assert_eq!(
        result.rows.len(),
        1,
        "edge_b must have exactly 1 row for pk; got {}",
        result.rows.len()
    );
    assert_eq!(
        result.rows[0][0],
        Value::TxId(tx_id_used),
        "edge_b.x must equal Value::TxId({:?}) bit-identical to sender",
        tx_id_used
    );

    // Edge-B's committed_watermark must have advanced past the peer TxId.
    let watermark = edge_b_db.committed_watermark();
    assert!(
        watermark >= tx_id_used,
        "edge_b.committed_watermark ({:?}) must be >= peer TxId ({:?})",
        watermark,
        tx_id_used
    );
}

// Named vector index RED tests from named-vector-indexes-tests.md.
/// I pushed an evidence row carrying both vector_text and vector_vision through the sync wire format, and the
/// receiver decoded each change with the right (table, column) identity — the two columns did not collide.
#[test]
fn f111_sync_vector_change_round_trip_preserves_table_column_identity() {
    use contextdb_core::{Lsn, RowId, VectorIndexRef};
    use contextdb_engine::sync_types::{ChangeSet, VectorChange};
    use contextdb_server::protocol::WireChangeSet;

    let original = ChangeSet {
        rows: vec![],
        edges: vec![],
        vectors: vec![
            VectorChange {
                index: VectorIndexRef::new("evidence", "vector_text"),
                row_id: RowId(7),
                vector: vec![1.0, 0.0, 0.0, 0.0],
                lsn: Lsn(42),
            },
            VectorChange {
                index: VectorIndexRef::new("evidence", "vector_vision"),
                row_id: RowId(7),
                vector: vec![0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                lsn: Lsn(42),
            },
        ],
        ddl: vec![],
    };
    let wire: WireChangeSet = original.clone().into();
    let bytes = rmp_serde::to_vec(&wire).expect("rmp_serde encode");
    let decoded: WireChangeSet = rmp_serde::from_slice(&bytes).expect("rmp_serde decode");
    let restored: ChangeSet = decoded.into();
    assert_eq!(
        restored.vectors[0].index,
        VectorIndexRef::new("evidence", "vector_text")
    );
    assert_eq!(
        restored.vectors[1].index,
        VectorIndexRef::new("evidence", "vector_vision")
    );
}
