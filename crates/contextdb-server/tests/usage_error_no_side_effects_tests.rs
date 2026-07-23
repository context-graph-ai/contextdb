//! Exit `2` means the invocation itself was wrong and nothing ran — no work
//! attempted. `contextdb-server` must never open (and, for a fresh path,
//! create) the database file before it validates that
//! `--show-ticket`/`--ticket-file` were paired with a sync endpoint rather
//! than a broker URL — a caller that branches on exit `2` to mean "safe to
//! retry, nothing changed on disk" must never find a brand-new database file
//! waiting for it.
//!
//! The mutually-exclusive-branch shape of `main.rs` means this scenario
//! (a broker URL plus a ticket-only flag) never reaches the code path that
//! binds a listening port — that branch requires `EndpointSpec::parse` to
//! recognize the endpoint as a sync-endpoint spec, which a broker URL never
//! is — so there is nothing observable to assert about port binding here
//! beyond the database file itself.

use std::process::Command;

fn unique_temp_dir(tag: &str) -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "cdb-server-usage-side-effects-{tag}-{}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create unique temp dir");
    dir
}

// A broker URL (`--nats-url`) combined with `--show-ticket` is a usage
// error (tickets exist only for sync-endpoint binds): the invocation is
// wrong, so the database must never be opened (and, since the path is
// fresh, created on disk) before that check runs.
#[test]
fn usage_error_before_ticket_flag_check_does_not_create_the_database() {
    let dir = unique_temp_dir("fresh");
    let db_path = dir.join("fresh.db");
    assert!(!db_path.exists(), "the path must start out absent");

    let output = Command::new(env!("CARGO_BIN_EXE_contextdb-server"))
        .arg("--db-path")
        .arg(&db_path)
        .arg("--tenant-id")
        .arg("acme")
        .arg("--nats-url")
        .arg("nats://localhost:4222")
        .arg("--show-ticket")
        .output()
        .expect("spawn contextdb-server");

    assert_eq!(
        output.status.code(),
        Some(2),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !db_path.exists(),
        "exit 2 must mean no work was attempted — the database file must never \
         be created before the ticket-flags/broker-URL check runs. Directory \
         contents: {:?}",
        std::fs::read_dir(&dir)
            .map(|entries| entries
                .filter_map(|e| e.ok().map(|e| e.file_name()))
                .collect::<Vec<_>>())
            .unwrap_or_default()
    );

    let _ = std::fs::remove_dir_all(&dir);
}
