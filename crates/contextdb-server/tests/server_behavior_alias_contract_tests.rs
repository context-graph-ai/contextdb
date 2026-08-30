//! Database location is the server's only bootstrap environment alias. All
//! behavior aliases are inert, including historic sync and resource spellings.

use std::path::Path;
use std::process::Command;

use contextdb_server::transport::peer_bind_spec;

const BEHAVIOR_ALIASES: [(&str, &str); 7] = [
    ("CONTEXTDB_TENANT_ID", "wrong-tenant"),
    ("CONTEXTDB_SYNC_ENDPOINT", "not-a-valid-sync-endpoint"),
    ("CONTEXTDB_SYNC_DEBOUNCE_MS", "not-a-number"),
    ("CONTEXTDB_RESPONSE_STAGING_BYTES", "not-a-number"),
    ("CONTEXTDB_PRE_ADMISSION_CONNECTIONS", "not-a-number"),
    ("CONTEXTDB_PRE_ADMISSION_BYTES", "not-a-number"),
    ("CONTEXTDB_REQUEST_READ_IDLE_MS", "not-a-number"),
];

fn explicit_command(root: &Path) -> Command {
    let endpoint = peer_bind_spec(&root.join("explicit.key"));
    let mut command = Command::new(env!("CARGO_BIN_EXE_contextdb-server"));
    command.args([
        "--db-path",
        ":memory:",
        "--tenant-id",
        "acme",
        "--sync-endpoint",
        &endpoint,
        "--response-staging-bytes",
        "7340032",
        "--pre-admission-connections",
        "3",
        "--pre-admission-bytes",
        "2097152",
        "--request-read-idle-ms",
        "1234",
        "--show-ticket",
    ]);
    command.env("RUST_LOG", "off");
    for (name, _) in BEHAVIOR_ALIASES {
        command.env_remove(name);
    }
    command
}

#[test]
fn tenant_and_sync_aliases_cannot_supply_behavior_configuration() {
    let root = tempfile::tempdir().expect("temporary directory");
    let endpoint = peer_bind_spec(&root.path().join("tenant.key"));
    let tenant_output = Command::new(env!("CARGO_BIN_EXE_contextdb-server"))
        .args([
            "--db-path",
            ":memory:",
            "--sync-endpoint",
            &endpoint,
            "--show-ticket",
        ])
        .env("CONTEXTDB_TENANT_ID", "must-not-be-read")
        .env_remove("CONTEXTDB_SYNC_ENDPOINT")
        .env("RUST_LOG", "off")
        .output()
        .expect("spawn server");
    let stderr = String::from_utf8_lossy(&tenant_output.stderr);
    assert_eq!(tenant_output.status.code(), Some(2), "{stderr}");
    assert!(stderr.contains("--tenant-id"), "{stderr}");

    let sync_output = Command::new(env!("CARGO_BIN_EXE_contextdb-server"))
        .args([
            "--db-path",
            ":memory:",
            "--tenant-id",
            "acme",
            "--show-ticket",
        ])
        .env("CONTEXTDB_SYNC_ENDPOINT", "not-a-valid-sync-endpoint")
        .env("TMPDIR", root.path())
        .env("RUST_LOG", "off")
        .output()
        .expect("spawn server");
    assert!(
        sync_output.status.success(),
        "sync endpoint environment alias must be inert: {}",
        String::from_utf8_lossy(&sync_output.stderr)
    );
}

#[test]
fn behavior_aliases_leave_explicit_invocation_bytes_and_exit_unchanged() {
    let root = tempfile::tempdir().expect("temporary directory");
    let baseline = explicit_command(root.path())
        .output()
        .expect("spawn clean explicit server invocation");
    assert!(
        baseline.status.success(),
        "clean explicit invocation failed: {}",
        String::from_utf8_lossy(&baseline.stderr)
    );
    let mut with_aliases = explicit_command(root.path());
    for (name, value) in BEHAVIOR_ALIASES {
        with_aliases.env(name, value);
    }
    let aliased = with_aliases
        .output()
        .expect("spawn aliased explicit server invocation");

    assert_eq!(aliased.status.code(), baseline.status.code());
    assert_eq!(aliased.stdout, baseline.stdout, "aliases changed stdout");
    assert_eq!(aliased.stderr, baseline.stderr, "aliases changed stderr");
}

#[test]
fn optional_behavior_aliases_cannot_supply_policy_configuration() {
    let root = tempfile::tempdir().expect("temporary directory");
    let endpoint = peer_bind_spec(&root.path().join("defaults.key"));
    let baseline = Command::new(env!("CARGO_BIN_EXE_contextdb-server"))
        .args([
            "--db-path",
            ":memory:",
            "--tenant-id",
            "acme",
            "--sync-endpoint",
            &endpoint,
            "--show-ticket",
        ])
        .env("RUST_LOG", "off")
        .env_remove("CONTEXTDB_SYNC_DEBOUNCE_MS")
        .env_remove("CONTEXTDB_RESPONSE_STAGING_BYTES")
        .env_remove("CONTEXTDB_PRE_ADMISSION_CONNECTIONS")
        .env_remove("CONTEXTDB_PRE_ADMISSION_BYTES")
        .env_remove("CONTEXTDB_REQUEST_READ_IDLE_MS")
        .output()
        .expect("spawn clean default-policy invocation");
    assert!(
        baseline.status.success(),
        "clean default-policy invocation failed: {}",
        String::from_utf8_lossy(&baseline.stderr)
    );

    let mut with_aliases = Command::new(env!("CARGO_BIN_EXE_contextdb-server"));
    with_aliases
        .args([
            "--db-path",
            ":memory:",
            "--tenant-id",
            "acme",
            "--sync-endpoint",
            &endpoint,
            "--show-ticket",
        ])
        .env("RUST_LOG", "off");
    for (name, value) in BEHAVIOR_ALIASES.into_iter().skip(2) {
        with_aliases.env(name, value);
    }
    let aliased = with_aliases
        .output()
        .expect("spawn default-policy invocation with optional aliases");
    assert_eq!(aliased.status.code(), baseline.status.code());
    assert_eq!(aliased.stdout, baseline.stdout, "aliases changed stdout");
    assert_eq!(aliased.stderr, baseline.stderr, "aliases changed stderr");
}

#[test]
fn database_path_bootstrap_alias_works_without_a_flag() {
    let root = tempfile::tempdir().expect("temporary directory");
    let db_path = root.path().join("from-env.db");
    let endpoint = peer_bind_spec(&root.path().join("env-db.key"));
    let output = Command::new(env!("CARGO_BIN_EXE_contextdb-server"))
        .args([
            "--tenant-id",
            "acme",
            "--sync-endpoint",
            &endpoint,
            "--show-ticket",
        ])
        .env("CONTEXTDB_DB_PATH", &db_path)
        .env("RUST_LOG", "off")
        .output()
        .expect("spawn server");
    assert!(
        output.status.success(),
        "database bootstrap alias failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        db_path.exists(),
        "environment-only database path was not used"
    );
}

#[test]
fn database_path_flag_wins_over_bootstrap_alias() {
    let root = tempfile::tempdir().expect("temporary directory");
    let env_path = root.path().join("must-not-exist.db");
    let mut command = explicit_command(root.path());
    command.env("CONTEXTDB_DB_PATH", &env_path);
    let output = command.output().expect("spawn server");
    assert!(output.status.success());
    assert!(
        !env_path.exists(),
        "--db-path must win over its environment alias"
    );
}

#[test]
fn shipped_smoke_passes_sync_configuration_as_flags() {
    let source = include_str!("../src/smoke_policy_journey.rs");
    for alias in [
        "CONTEXTDB_SYNC_ENDPOINT",
        "CONTEXTDB_TENANT_ID",
        "CONTEXTDB_SYNC_DEBOUNCE_MS",
    ] {
        assert!(
            !source.contains(alias),
            "shipped smoke must not set behavior alias {alias}"
        );
    }
    for flag in ["--sync-endpoint", "--tenant-id", "--sync-debounce-ms"] {
        assert_eq!(
            source.matches(&format!("\"{flag}\"")).count(),
            2,
            "shipped smoke must pass {flag} at both command construction sites"
        );
    }
}
