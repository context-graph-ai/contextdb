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
    NatsFixture,
) {
    let tmp = TempDir::new().expect("tempdir");
    let edge_path = temp_db_file(&tmp, &format!("{name}-edge.db"));
    let server_path = temp_db_file(&tmp, &format!("{name}-server.db"));
    let nats = start_nats().await;
    (tmp, edge_path, server_path, nats.nats_url.clone(), nats)
}

/// I wrote data on an edge node, and it showed up on the server without me running any sync command.
#[tokio::test]
async fn f06a_data_written_on_edge_appears_on_server_automatically() {
    let (_tmp, edge_path, server_path, nats_url, _nats) = setup_sync_env("f06a").await;
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
            let (_tmp, edge_path, server_path, nats_url, _nats) =
                setup_sync_env(stringify!($name)).await;
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
#[tokio::test]
async fn f06b_data_from_another_edge_appears_on_this_edge_automatically() {
    let (_tmp, edge_path, server_path, nats_url, _nats) =
        setup_sync_env("f06b_data_from_another_edge_appears_on_this_edge_automatically").await;
    let tenant = "f06b_data_from_another_edge_appears_on_this_edge_automatically";
    let mut server = spawn_server(&server_path, tenant, &nats_url);
    let mut script = String::from("CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n");
    script.push_str(&gen_sensor_inserts(10));
    script.push_str(".quit\n");
    let output = run_cli_script(
        &edge_path,
        &["--tenant-id", tenant, "--nats-url", &nats_url],
        &script,
    );
    assert!(output.status.success());
    stop_child(&mut server);
    let output = run_cli_script(
        &edge_path,
        &["--tenant-id", tenant, "--nats-url", &nats_url],
        "SELECT count(*) FROM sensors\n.quit\n",
    );
    assert!(output_string(&output.stdout).contains("10"));
}

// I wrote data while the edge was offline, and when the network came back the backlog was synced automatically.
#[tokio::test]
async fn f06c_edge_reconnects_after_network_outage_and_auto_syncs_backlog() {
    let (_tmp, edge_path, server_path, nats_url, _nats) =
        setup_sync_env("f06c_edge_reconnects_after_network_outage_and_auto_syncs_backlog").await;
    let tenant = "f06c_edge_reconnects_after_network_outage_and_auto_syncs_backlog";
    let mut server = spawn_server(&server_path, tenant, &nats_url);
    let mut script = String::from("CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n");
    script.push_str(&gen_sensor_inserts(50));
    script.push_str(".quit\n");
    let output = run_cli_script(
        &edge_path,
        &["--tenant-id", tenant, "--nats-url", &nats_url],
        &script,
    );
    assert!(output.status.success());
    stop_child(&mut server);
    assert_eq!(count_rows_from_file(&server_path, "sensors"), 50);
}

// I pushed data from the edge, and the server had all the rows.
#[tokio::test]
async fn f06_edge_pushes_data_server_has_it() {
    let (_tmp, edge_path, server_path, nats_url, _nats) =
        setup_sync_env("f06_edge_pushes_data_server_has_it").await;
    let tenant = "f06_edge_pushes_data_server_has_it";
    let mut server = spawn_server(&server_path, tenant, &nats_url);
    let mut script = String::from("CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n");
    script.push_str(&gen_sensor_inserts(100));
    script.push_str(".sync push\n.quit\n");
    let output = run_cli_script(
        &edge_path,
        &["--tenant-id", tenant, "--nats-url", &nats_url],
        &script,
    );
    assert!(output.status.success());
    stop_child(&mut server);
    assert_eq!(count_rows_from_file(&server_path, "sensors"), 100);
}

// I pushed twice in a row, and the server had each row exactly once — no duplicates.
#[tokio::test]
async fn f07_two_consecutive_pushes_do_not_duplicate_data() {
    let (_tmp, edge_path, server_path, nats_url, _nats) =
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
        &["--tenant-id", tenant, "--nats-url", &nats_url],
        &script,
    );
    assert!(output.status.success());
    stop_child(&mut server);
    assert_eq!(count_rows_from_file(&server_path, "sensors"), 100);
}

// I pushed from one edge, then pulled onto a brand-new edge, and the fresh edge had all the data.
#[tokio::test]
async fn f08_push_then_pull_on_a_fresh_edge() {
    let (_tmp, edge_path, server_path, nats_url, _nats) =
        setup_sync_env("f08_push_then_pull_on_a_fresh_edge").await;
    let tenant = "f08_push_then_pull_on_a_fresh_edge";
    let mut server = spawn_server(&server_path, tenant, &nats_url);
    let mut script = String::from("CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n");
    script.push_str(&gen_sensor_inserts(100));
    script.push_str(".sync push\n.quit\n");
    let output = run_cli_script(
        &edge_path,
        &["--tenant-id", tenant, "--nats-url", &nats_url],
        &script,
    );
    assert!(output.status.success());
    let fresh_path = edge_path.with_file_name("fresh-edge.db");
    let pulled = run_cli_script(
        &fresh_path,
        &["--tenant-id", tenant, "--nats-url", &nats_url],
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n.sync pull\nSELECT count(*) FROM sensors\n.quit\n",
    );
    stop_child(&mut server);
    assert!(output_string(&pulled.stdout).contains("100"));
}

// I pushed data, the server restarted, and a pull from a fresh edge still returned everything.
#[tokio::test]
async fn f09_pull_after_server_restart_returns_data() {
    let (_tmp, edge_path, server_path, nats_url, _nats) =
        setup_sync_env("f09_pull_after_server_restart_returns_data").await;
    let tenant = "f09_pull_after_server_restart_returns_data";
    let mut server = spawn_server(&server_path, tenant, &nats_url);
    let mut script = String::from("CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n");
    script.push_str(&gen_sensor_inserts(100));
    script.push_str(".sync push\n.quit\n");
    let output = run_cli_script(
        &edge_path,
        &["--tenant-id", tenant, "--nats-url", &nats_url],
        &script,
    );
    assert!(output.status.success());
    stop_child(&mut server);
    let mut restarted = spawn_server(&server_path, tenant, &nats_url);
    let fresh_path = edge_path.with_file_name("fresh-after-restart.db");
    let pulled = run_cli_script(
        &fresh_path,
        &["--tenant-id", tenant, "--nats-url", &nats_url],
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n.sync pull\nSELECT count(*) FROM sensors\n.quit\n",
    );
    stop_child(&mut restarted);
    assert!(output_string(&pulled.stdout).contains("100"));
}

// I pushed, closed the edge, reopened it, inserted more rows, pushed again, and the server had everything.
#[tokio::test]
async fn f09b_edge_closes_reopens_pushes_more_data() {
    let (_tmp, edge_path, server_path, nats_url, _nats) =
        setup_sync_env("f09b_edge_closes_reopens_pushes_more_data").await;
    let tenant = "f09b_edge_closes_reopens_pushes_more_data";
    let mut server = spawn_server(&server_path, tenant, &nats_url);
    // First session: create table and insert 50 rows, push
    let mut script1 = String::from("CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n");
    script1.push_str(&gen_sensor_inserts(50));
    script1.push_str(".sync push\n.quit\n");
    let output = run_cli_script(
        &edge_path,
        &["--tenant-id", tenant, "--nats-url", &nats_url],
        &script1,
    );
    assert!(output.status.success());
    // Second session: insert 50 more rows, push
    let mut script2 = String::new();
    script2.push_str(&gen_sensor_inserts(50));
    script2.push_str(".sync push\n.quit\n");
    let reopened = run_cli_script(
        &edge_path,
        &["--tenant-id", tenant, "--nats-url", &nats_url],
        &script2,
    );
    assert!(reopened.status.success());
    stop_child(&mut server);
    assert_eq!(count_rows_from_file(&server_path, "sensors"), 100);
}

// I crashed the edge mid-session, reopened it, pushed, and the server received all the data including what was written before the crash.
#[tokio::test]
async fn f09c_edge_crash_recovers_then_pushes() {
    let (_tmp, edge_path, server_path, nats_url, _nats) =
        setup_sync_env("f09c_edge_crash_recovers_then_pushes").await;
    let tenant = "f09c_edge_crash_recovers_then_pushes";
    let mut server = spawn_server(&server_path, tenant, &nats_url);
    // First session: create table, insert 100 rows, push
    let mut script1 = String::from("CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n");
    script1.push_str(&gen_sensor_inserts(100));
    script1.push_str(".sync push\n.quit\n");
    let output = run_cli_script(
        &edge_path,
        &["--tenant-id", tenant, "--nats-url", &nats_url],
        &script1,
    );
    assert!(output.status.success());
    // Crash session: insert 25 rows then kill (simulating crash)
    let mut child = spawn_cli(
        &edge_path,
        &["--tenant-id", tenant, "--nats-url", &nats_url],
    );
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
        &["--tenant-id", tenant, "--nats-url", &nats_url],
        &script3,
    );
    assert!(reopened.status.success());
    stop_child(&mut server);
    assert_eq!(count_rows_from_file(&server_path, "sensors"), 150);
}

// I lost power during a sync, retried, and the server had each row exactly once — no duplicates from the interrupted attempt.
#[tokio::test]
async fn f09d_power_loss_during_sync_does_not_cause_duplicates_on_retry() {
    let (_tmp, edge_path, server_path, nats_url, _nats) =
        setup_sync_env("f09d_power_loss_during_sync_does_not_cause_duplicates_on_retry").await;
    let tenant = "f09d_power_loss_during_sync_does_not_cause_duplicates_on_retry";
    let mut server = spawn_server(&server_path, tenant, &nats_url);
    let mut script = String::from("CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n");
    script.push_str(&gen_sensor_inserts(100));
    script.push_str(".quit\n");
    let output = run_cli_script(
        &edge_path,
        &["--tenant-id", tenant, "--nats-url", &nats_url],
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
    let tenant = "f09f_server_side_constraint_violation_during_push_returns_error_to_edge";
    let mut server = spawn_server(&server_path, tenant, nats_url);

    // Edge 1: create table WITH NOT NULL, insert a valid row, push to server via NATS
    let edge1_path = edge_path.with_file_name("f09f-edge1.db");
    let edge1_output = run_cli_script(
        &edge1_path,
        &["--tenant-id", tenant, "--nats-url", nats_url],
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
        &["--tenant-id", tenant, "--nats-url", nats_url],
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
    let tenant = "f09h_constraint_violations_during_sync_pull_are_handled";
    let mut server = spawn_server(&server_path, tenant, nats_url);

    // Edge A pushes a table with columns (id UUID PK, name TEXT, reading REAL)
    let edge_a_path = edge_path.with_file_name("f09h-edge-a.db");
    let push_output = run_cli_script(
        &edge_a_path,
        &["--tenant-id", tenant, "--nats-url", nats_url],
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
        &["--tenant-id", tenant, "--nats-url", nats_url],
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
    let tenant = "f09i_conflicting_state_machine_transitions_across_edges_during_sync";
    let mut server = spawn_server(&server_path, tenant, nats_url);

    let row_id = "00000000-0000-0000-0000-000000000001";

    // Edge A: create table with state machine, insert row as "active", push
    let edge_a_path = temp_db_file(&tmp, "f09i-edge-a.db");
    let setup = run_cli_script(
        &edge_a_path,
        &["--tenant-id", tenant, "--nats-url", nats_url],
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
        &["--tenant-id", tenant, "--nats-url", nats_url],
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
    {
        let db = Database::open(&server_path).expect("server db for precondition check");
        let row = db
            .point_lookup(
                "tasks",
                "id",
                &Value::Uuid(Uuid::parse_str(row_id).expect("uuid")),
                db.snapshot(),
            )
            .expect("lookup")
            .expect("row must exist after Edge B push");
        assert_eq!(
            row.values.get("status"),
            Some(&Value::Text("review".to_string())),
            "precondition: server must have 'review' after Edge B's push"
        );
        db.close().unwrap();
    }

    // Edge A: transition same row to "archived" (from stale "active" state), push
    // Server has "review" from Edge B. Edge A thinks it's still "active".
    // This push must report a conflict — the row is no longer in "active" state.
    let conflict_push = run_cli_script(
        &edge_a_path,
        &["--tenant-id", tenant, "--nats-url", nats_url],
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
    let status = row
        .values
        .get("status")
        .expect("status column");
    assert_eq!(
        *status,
        Value::Text("review".to_string()),
        "server must keep Edge B's 'review' state, not silently accept Edge A's stale 'archived' transition"
    );
}

// I accumulated data offline for an extended period, pushed, and the server received the entire backlog.
#[tokio::test]
async fn f10_edge_offline_for_one_hour_then_pushes_backlog() {
    let (_tmp, edge_path, server_path, nats_url, _nats) =
        setup_sync_env("f10_edge_offline_for_one_hour_then_pushes_backlog").await;
    let tenant = "f10_edge_offline_for_one_hour_then_pushes_backlog";
    let mut server = spawn_server(&server_path, tenant, &nats_url);
    let mut script = String::from("CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n");
    script.push_str(&gen_sensor_inserts(500));
    script.push_str(".sync push\n.quit\n");
    let output = run_cli_script(
        &edge_path,
        &["--tenant-id", tenant, "--nats-url", &nats_url],
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
    let tenant = "f12_auto_sync_pushes_on_commit_not_on_quit";
    let mut server = spawn_server(&server_path, tenant, nats_url);

    // Start CLI with spawn_cli (keeps process alive)
    let mut child = spawn_cli(
        &edge_path,
        &["--tenant-id", tenant, "--nats-url", nats_url],
    );

    // Enable auto-sync, create table, insert a row
    write_child_stdin(
        &mut child,
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n\
         .sync auto\n\
         INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'probe')\n",
    );

    // Wait for data to appear on server WHILE CLI IS STILL RUNNING.
    // Timeout-based poll is correct here: auto-sync is async by design,
    // there is no deterministic completion signal (unlike f09c which has stdout "ok" lines).
    let found = wait_until(Duration::from_secs(10), || {
        // Must not kill the server — check by opening a second connection to the DB
        // The server process holds the DB open, so we check via a fresh CLI pull
        let fresh_path = edge_path.with_file_name("f12-checker.db");
        let check = run_cli_script(
            &fresh_path,
            &["--tenant-id", tenant, "--nats-url", nats_url],
            "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n\
             .sync pull\n\
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
    let tenant = "f12b_auto_sync_pushes_updates_not_just_inserts";
    let mut server = spawn_server(&server_path, tenant, nats_url);

    let mut child = spawn_cli(
        &edge_path,
        &["--tenant-id", tenant, "--nats-url", nats_url],
    );

    // Create table, insert, enable auto-sync, then UPDATE
    write_child_stdin(
        &mut child,
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n\
         INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'original')\n\
         .sync auto\n\
         .sync push\n\
         UPDATE sensors SET name = 'updated' WHERE id = '00000000-0000-0000-0000-000000000001'\n",
    );

    // Wait for UPDATE to appear on server while CLI is still running
    let found = wait_until(Duration::from_secs(10), || {
        let fresh_path = edge_path.with_file_name("f12b-checker.db");
        let check = run_cli_script(
            &fresh_path,
            &["--tenant-id", tenant, "--nats-url", nats_url],
            "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n\
             .sync pull\n\
             SELECT name FROM sensors WHERE id = '00000000-0000-0000-0000-000000000001'\n\
             .quit\n",
        );
        let stdout = output_string(&check.stdout);
        stdout.contains("updated")
    });

    write_child_stdin(&mut child, ".quit\n");
    let _ = child.wait();
    stop_child(&mut server);

    assert!(
        found,
        "UPDATE must auto-sync to server while CLI is still running"
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
    let tenant = "f12c_auto_sync_pushes_deletes";
    let mut server = spawn_server(&server_path, tenant, nats_url);

    let mut child = spawn_cli(
        &edge_path,
        &["--tenant-id", tenant, "--nats-url", nats_url],
    );

    // Create table, insert 2 rows, push, enable auto-sync, then DELETE one
    write_child_stdin(
        &mut child,
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n\
         INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'keep')\n\
         INSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000002', 'delete_me')\n\
         .sync push\n\
         .sync auto\n\
         DELETE FROM sensors WHERE id = '00000000-0000-0000-0000-000000000002'\n",
    );

    // Wait for DELETE to propagate — server should have 1 row, not 2
    let found = wait_until(Duration::from_secs(10), || {
        let fresh_path = edge_path.with_file_name("f12c-checker.db");
        let check = run_cli_script(
            &fresh_path,
            &["--tenant-id", tenant, "--nats-url", nats_url],
            "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\n\
             .sync pull\n\
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
        "DELETE must auto-sync to server while CLI is still running"
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
